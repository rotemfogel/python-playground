import json
import os
from datetime import datetime

import requests
import smart_open
from airflow import LoggingMixin
from dotenv import load_dotenv, find_dotenv
from pendulum import Pendulum

load_dotenv(find_dotenv())
_endpoint = os.getenv('ENDPOINT')
_secret = os.getenv('SECRET')
_data_bucket = os.getenv('DATA_BUCKET')
_active_last_ts: str = os.getenv('ACTIVE_CAMPAIGNS_LAST_TS')
_campaign_ids: list = json.loads(os.getenv('CAMPAIGN_IDS'))


class ActiveCampaignsBaseOperator(LoggingMixin):
    _schema = {'db_report': 'dbr'}
    _bucket = _data_bucket
    _database = _schema['db_report']
    _active_campaigns_conf = {
        'endpoint': _endpoint,
        'secret': _secret
    }
    _active_campaigns_last_ts: dict = json.loads(_active_last_ts)
    _datetime_format = '%Y-%m-%d %H:%M:%S'

    _campaign_id = 'campaignid'
    _message_id = 'messageid'

    template_fields = ('api_prefix', 'api_action', 'data_point', 'sort_key')

    def __init__(self,
                 api_prefix: str,
                 data_point: str = None,
                 api_action: str = None,
                 sort_key: str = None,
                 last_ts_ind: bool = False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._api_prefix = api_prefix
        self._api_action = api_action
        self._data_point = data_point
        self._sort_key = sort_key
        self._last_ts_ind = last_ts_ind
        self._endpoint: str = self._active_campaigns_conf['endpoint']
        self._secret: str = self._active_campaigns_conf['secret']

    def _build_params(self):
        params = {'api_key': self._secret, 'api_output': 'json'}
        if self._sort_key:
            params.update({'sort': self._sort_key, 'sort_direction': 'DESC'})
        # non-admin API
        if self._api_action:
            params.update({'api_action': self._api_action})
        return params

    def _build_endpoint(self):
        return self._endpoint + self._api_prefix

    def _get_records(self, endpoint: str, params: dict) -> list:
        response = requests.request('GET', endpoint, params=params)
        json_response = response.json()
        result_code = json_response.get('result_code', -1)
        # no rows found
        if result_code == 0:
            return []
        return json_response.get(self._data_point, []) if self._data_point else json_response

    def _save(self, context, records: list) -> None:
        if records:
            api_action = self._api_action
            execution_date = context['ti'].execution_date if context else Pendulum.now()
            full_date = execution_date.format('%Y-%m-%dT%H:%M:%S')
            date = execution_date.format('%Y-%m-%d')
            if self._last_ts_ind:
                hour = execution_date.format('%H')
                uri: str = 's3://{bucket}/{schema}/active_campaigns/{table}/' \
                           'date_={date}/hour={hour}/{file_name}.json.gz'.format(bucket=self._bucket,
                                                                                 schema=self._database,
                                                                                 table=self._api_action,
                                                                                 date=date,
                                                                                 hour=hour,
                                                                                 file_name='{}_{}'.format(
                                                                                     self._api_action, full_date))
            else:
                # get then last uri path (i.e. /a/b/c -> c)
                api_action = self._api_prefix.split('/')[-1]
                uri: str = 's3://{bucket}/{schema}/active_campaigns/{table}/' \
                           'date_={date}/{file_name}.json.gz'.format(bucket=self._bucket,
                                                                     schema=self._database,
                                                                     table=api_action,
                                                                     date=date,
                                                                     file_name=api_action)

            with smart_open.open(uri=uri, mode='wb') as s3_file:
                self.log.info('About to write response to {}'.format(uri))
                for record in records:
                    if not record:
                        continue
                    s3_file.write((json.dumps(record) + '\n').encode())

                self.log.info('Uploaded {} to S3'.format(api_action))


class ActiveCampaignsCampaignOperator(ActiveCampaignsBaseOperator):
    _campaign_message = 'campaignMessage'
    template_fields = ('api_prefix', 'data_point')

    def __init__(self,
                 api_prefix: str,
                 data_point: str,
                 *args,
                 **kwargs):
        super().__init__(api_prefix, data_point, *args, **kwargs)

    def execute(self, context=None):
        params = self._build_params()
        endpoint = self._build_endpoint()
        rows = self._get_records(endpoint, params)
        records = []
        for row in rows:
            if row:
                records.append(row)

        self._save(context, records)

        # get the campaign messages from the campaigns
        ##  campaign_ids = []
        links = list(map(lambda x: str(x['links'][self._campaign_message]), records))
        for link in links:
            self._api_prefix = link[len(self._endpoint):len(link)]
            self._data_point = self._campaign_message
            params = self._build_params()
            endpoint = self._build_endpoint()
            row = self._get_records(endpoint, params)
            if row:
                ## campaign_ids.append({self._campaign_id: row[self._campaign_id],
                _campaign_ids.append({self._campaign_id: row[self._campaign_id],
                                      self._message_id: row[self._message_id]})

        ## if campaign_ids:
        ##   context['ti'].xcom_push(key=super().dag_id, value=campaign_ids)


class ActiveCampaignsDeltaOperator(ActiveCampaignsBaseOperator):
    template_fields = ('api_prefix', 'api_action', 'data_point', 'sort_key', 'last_ts_ind')

    def __init__(self,
                 api_prefix: str,
                 data_point: str,
                 api_action: str = None,
                 sort_key: str = None,
                 *args,
                 **kwargs):
        super().__init__(api_prefix, data_point, api_action, sort_key, True, *args, **kwargs)
        self._default_campaign_last_ts = {self._api_action: '2020-01-01 00:00:00'}

    def execute(self, context=None):
        campaigns = _campaign_ids
        for campaign in campaigns:
            records = []
            offset: int = 100
            limit: int = offset
            params = self._build_params()
            endpoint = self._build_endpoint()
            params.update({self._campaign_id: campaign[self._campaign_id],
                           self._message_id: campaign[self._message_id]})
            # get the last watermark for the campaign/api
            campaign_last_ts = self._active_campaigns_last_ts.get(campaign[self._campaign_id],
                                                                  self._default_campaign_last_ts)
            last_watermark: datetime = datetime.strptime(
                campaign_last_ts.get(self._api_action, self._default_campaign_last_ts[self._api_action]),
                self._datetime_format)
            # set the highest ts to last watermark
            high_ts = last_watermark
            while True:
                self.log.info('getting data for campaign: {}, message: {}, offset: {}, limit: {}'.format(
                    campaign[self._campaign_id],
                    campaign[self._message_id],
                    offset,
                    limit))
                params.update({'offest': str(offset), 'limit': str(limit)})
                rows = self._get_records(endpoint, params)
                if not rows:
                    break
                keys = rows.keys()
                for key in keys:
                    if key not in ['result_code', 'result_message', 'result_output']:
                        row = rows[key]
                        if row:
                            # if we have not reached the last ts (previous execution)
                            timestamp = row.get('tstamp', None)
                            if not timestamp:
                                timestamp = row['info'][0]['tstamp']
                            record_time: datetime = datetime.strptime(timestamp, self._datetime_format)
                            # update the variable with highest timestamp
                            if record_time > high_ts:
                                high_ts = record_time
                            if record_time <= last_watermark:
                                break
                            records.append(row)
                if len(keys) < limit:
                    break
                offset += limit
            # save the campaign result
            self._save(context, records)
            if high_ts > last_watermark:
                self._save_last_ts(campaign[self._campaign_id], datetime.strftime(high_ts, self._datetime_format))

    def _save_last_ts(self, campaign_id: str, last_ts: str) -> None:
        campaign_last_ts = self._active_campaigns_last_ts.get(campaign_id, {})
        campaign_last_ts.update({self._api_action: last_ts})
        self._active_campaigns_last_ts.update({campaign_id: campaign_last_ts})
        ## Variable.set('active_campaigns_last_ts', self._active_campaigns_last_ts, deserialize_json=True)
        self.log.info('setting active_campaigns_last_ts -> {}'.format(json.dumps(self._active_campaigns_last_ts)))


def main():
    tasks = json.loads(os.getenv('ACTIVE_CAMPAIGNS_TASKS'))

    task = tasks.pop(0)
    task_id = task['data_point']
    # make sure it's campaigns - we need the output of the IDs for later
    assert (task_id == 'campaigns')
    ac = ActiveCampaignsCampaignOperator(api_prefix=task['api_prefix'],
                                         data_point=task.get('data_point', None),
                                         api_action=task.get('api_action', None),
                                         sort_key=task.get('sort_key', None))
    ac.execute()
    for task in tasks:
        task_id = task['api_action']
        ac = ActiveCampaignsDeltaOperator(api_prefix=task['api_prefix'],
                                          data_point=task.get('data_point', None),
                                          api_action=task.get('api_action', None),
                                          sort_key=task.get('sort_key', None))
        ac.execute()

    # if _active_campaigns_last_ts:
    #     f = open(".env", "r+")
    #     lines = f.readlines()
    #     if lines:
    #         w = open("env", "w+")
    #         for line in lines:
    #             w.write(line)
    #         w.write('ACTIVE_CAMPAIGNS_LAST_TS={}'.format(json.dumps(_active_campaigns_last_ts)))
    #         w.close()
    #     f.close()


if __name__ == '__main__':
    main()
