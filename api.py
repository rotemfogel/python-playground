import json
import os
import urllib

import requests
from airflow import LoggingMixin
from dotenv import load_dotenv, find_dotenv
from pendulum import Pendulum
from smart_open import open

load_dotenv(find_dotenv())
_endpoint = os.getenv('ENDPOINT')
_secret = os.getenv('SECRET')
_data_bucket = os.getenv('DATA_BUCKET')


class ActiveCampaignsPaginateOperator(LoggingMixin):
    _schema = {'db_report': 'dbr'}
    _bucket = _data_bucket
    _database = _schema['db_report']
    _active_campaigns_conf = {
        'endpoint': _endpoint,
        'secret': _secret
    }

    template_fields = ('api_prefix', 'api_action', 'data_point', 'full')  # type: Iterable[str]

    def __init__(self,
                 api_prefix: str,
                 api_action: str,
                 data_point: str,
                 full: bool = False,
                 ids: list = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self._api_prefix = api_prefix
        self._api_action = api_action
        self._data_point = data_point
        self._full = full
        self._endpoint: str = self._active_campaigns_conf['endpoint']
        self._secret: str = self._active_campaigns_conf['secret']
        self._ids = ids

    @property
    def execute(self):
        records = []
        offset: int = 100
        limit: int = offset
        count: int = 0
        while True:
            params = {'api_action': self._api_action, 'api_key': self._secret, 'api_output': 'json'}
            if self._full:
                params.update({'offest': str(offset), 'limit': str(limit)})

            query_params = '&'.join(
                list(
                    map(lambda kv: ('{}={}'.format(urllib.parse.quote(kv[0]), urllib.parse.quote(kv[1]))),
                        params.items())))
            endpoint = '?'.join([self._endpoint + self._api_prefix, query_params])
            response = requests.request('GET', endpoint, data={})
            json_response = response.json()
            count += json_response['cnt']

            responses = json_response[self._data_point]
            for response in responses:
                records.append(response)
            if json_response['total'] == count:
                break
            if self._full:
                offset += limit

        if records:
            if self._full:
                uri: str = 's3://{bucket}/{schema}/active_campaigns/{table}/' \
                           '{file_name}.json.gz'.format(bucket=self._bucket, schema=self._database,
                                                        table=self._api_action, file_name=self._api_action)
            else:
                execution_date = Pendulum.now
                full_date = execution_date.format('%Y-%m-%dT%H:%M:%S')
                date = execution_date.format('%Y-%m-%d')
                hour = execution_date.format('%H')
                uri: str = 's3://{bucket}/{schema}/active_campaigns/{table}/' \
                           'date_={date}/hour={hour}/{file_name}.json.gz'.format(bucket=self._bucket,
                                                                                 schema=self._database,
                                                                                 table=self._api_action, date=date,
                                                                                 hour=hour,
                                                                                 file_name='{}_{}'.format(
                                                                                     self._api_action, full_date))
            with open(uri=uri, mode='wb') as s3_file:
                self.log.info('About to write response to {}'.format(uri))
                for record in records:
                    if not records:
                        continue
                    s3_file.write((json.dumps(record) + '\n').encode())

                self.log.info('Uploaded {} to S3'.format(self._api_action))

        if super().do_xcom_push:
            return [(record['id'], record['messageid']) for record in records]


def main():
    admin_api = '/admin/api.php'
    tasks = [
        {'api_prefix': admin_api, 'api_action': 'campaign_paginator', 'extract_point': 'rows', 'full': True},
        {'api_prefix': admin_api, 'api_action': 'campaign_report_open_list', 'extract_point': ''},
        {'api_prefix': admin_api, 'api_action': 'campaign_report_link_list', 'extract_point': ''},
    ]
    ids = []
    for task in tasks:
        task_id = task['api_action']
        if task_id == 'campaign_paginator':
            ac = ActiveCampaignsPaginateOperator(
                api_prefix=task['api_prefix'],
                api_action=task_id,
                data_point=task['extract_point'],
                full=task.get('full', None))
        else:
            ac = ActiveCampaignsPaginateOperator(
                api_prefix=task['api_prefix'],
                api_action=task_id,
                data_point=task['extract_point'],
                full=task.get('full', None))
        ids = ac.execute


# Using the special variable
# __name__
if __name__ == "__main__":
    main()
