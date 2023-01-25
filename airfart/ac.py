import json
import os
from abc import ABC
from copy import deepcopy
from datetime import timedelta, datetime

import requests
import smart_open
from dotenv import load_dotenv
from pendulum import DateTime

load_dotenv()


class ActiveCampaignBaseOperator(ABC):
    _active_campaign_conf: dict = json.loads(os.getenv("ACTIVE_CAMPAIGN_CONF"))
    _active_campaign_last_ts: dict = json.loads(os.getenv("ACTIVE_CAMPAIGN_LAST_TS"))
    _bucket: str = os.getenv("DATA_BUCKET")
    _database: str = os.getenv("DATABASE")
    _datetime_format: str = "%Y-%m-%d %H:%M:%S"

    _campaign_id: str = "campaignid"
    _message_id: str = "messageid"
    _default_start_time = "2020-01-01 00:00:00"

    def __init__(
        self,
        api_prefix: str,
        data_point: str = None,
        api_action: str = None,
        sort_key: str = None,
        last_ts_ind: bool = False,
        *args,
        **kwargs,
    ):
        self._api_prefix = api_prefix
        self._api_action = api_action
        self._data_point = data_point
        self._sort_key = sort_key
        self._last_ts_ind = last_ts_ind
        self._endpoint: str = self._active_campaign_conf["endpoint"]
        self._secret: str = self._active_campaign_conf["secret"]

    def _build_params(self):
        params = {"api_key": self._secret, "api_output": "json"}
        if self._sort_key:
            params.update({"sort": self._sort_key, "sort_direction": "DESC"})
        # non-admin API
        if self._api_action:
            params.update({"api_action": self._api_action})
        return params

    def _build_endpoint(self):
        return self._endpoint + self._api_prefix

    def _get_records(self, endpoint: str, params: dict) -> list:
        response = requests.request("GET", endpoint, params=params)
        json_response = response.json()
        result_code = json_response.get("result_code", -1)
        # no rows found
        if result_code == 0:
            return []
        return (
            json_response.get(self._data_point, [])
            if self._data_point
            else json_response
        )

    def _save(self, context: dict, records: list) -> None:
        api_action = self._api_action
        execution_date = context["execution_date"]
        full_date = execution_date.fmt("%Y-%m-%dT%H:%M:%S")
        date = execution_date.fmt("%Y-%m-%d")

        if self._last_ts_ind:
            hour = execution_date.fmt("%H")
            uri: str = (
                "s3://{bucket}/{schema}/base_{table}/"
                "date_={date}/hour={hour}/{file_name}.json.gz".format(
                    bucket=self._bucket,
                    schema=self._database,
                    table=self._api_action,
                    date=date,
                    hour=hour,
                    file_name="{}_{}".format(self._api_action, full_date),
                )
            )
        else:
            # get then last uri path (i.e. /a/b/c -> c)
            api_action = self._api_prefix.split("/")[-1]
            uri: str = (
                "s3://{bucket}/{schema}/base_{table}/"
                "date_={date}/{file_name}.json.gz".format(
                    bucket=self._bucket,
                    schema=self._database,
                    table=api_action,
                    date=date,
                    file_name=api_action,
                )
            )

        with smart_open.open(uri=uri, mode="wb") as s3_file:
            print("About to write response to {}".format(uri))
            for record in records:
                if not record:
                    continue
                s3_file.write((json.dumps(record) + "\n").encode())

            print("Uploaded {} to S3".format(api_action))


class ActiveCampaignCampaignOperator(ActiveCampaignBaseOperator):
    _campaign_message = "campaignMessage"

    def __init__(
        self,
        api_prefix: str,
        data_point: str,
        execution_timeout: timedelta = timedelta(
            minutes=5
        ),  # based on a max of 24s, of 524 runs
        *args,
        **kwargs,
    ):
        super().__init__(
            api_prefix=api_prefix,
            data_point=data_point,
            execution_timeout=execution_timeout,
            *args,
            **kwargs,
        )

    def execute(self, context):
        params = self._build_params()
        endpoint = self._build_endpoint()
        rows = self._get_records(endpoint, params)
        records = []
        for row in rows:
            if row:
                records.append(row)

        if records:
            self._save(context, records)

        # get the campaign messages from the campaigns
        campaign_ids = []
        links = list(map(lambda x: str(x["links"][self._campaign_message]), records))
        for link in links:
            self._api_prefix = link[len(self._endpoint) : len(link)]
            self._data_point = self._campaign_message
            params = self._build_params()
            endpoint = self._build_endpoint()
            row = self._get_records(endpoint, params)
            if row:
                campaign_ids.append(
                    {
                        self._campaign_id: row[self._campaign_id],
                        self._message_id: row[self._message_id],
                    }
                )

        # push through xcom
        if campaign_ids:
            context.update({"campaign_ids": campaign_ids})


class ActiveCampaignDeltaOperator(ActiveCampaignBaseOperator):
    def __init__(
        self,
        api_prefix: str,
        data_point: str,
        api_action: str = None,
        sort_key: str = None,
        execution_timeout: timedelta = timedelta(
            minutes=5
        ),  # based on a max of 22s, of 526 runs
        *args,
        **kwargs,
    ):
        super().__init__(
            api_prefix=api_prefix,
            data_point=data_point,
            api_action=api_action,
            sort_key=sort_key,
            execution_timeout=execution_timeout,
            last_ts_ind=True,
            *args,
            **kwargs,
        )
        # set the default watermark for this api to beginning of the year
        self._default_campaign_last_ts = {self._api_action: self._default_start_time}

    def execute(self, context: dict):
        campaign_pairs = context["campaign_ids"]
        for campaign in campaign_pairs:
            records = []
            offset: int = 100
            limit: int = offset
            params = self._build_params()
            endpoint = self._build_endpoint()
            params.update(
                {
                    self._campaign_id: campaign[self._campaign_id],
                    self._message_id: campaign[self._message_id],
                }
            )
            # get the last watermark for the campaign/api
            campaign_last_ts = self._active_campaign_last_ts.get(
                campaign[self._campaign_id], self._default_campaign_last_ts
            )
            # convert to datetime
            last_watermark: datetime = datetime.strptime(
                campaign_last_ts.get(
                    self._api_action, self._default_campaign_last_ts[self._api_action]
                ),
                self._datetime_format,
            )
            # copy the highest ts to last watermark
            high_ts = deepcopy(last_watermark)
            while True:
                print(
                    "getting data for campaign: {}, message: {}, offset: {}, limit: {}".format(
                        campaign[self._campaign_id],
                        campaign[self._message_id],
                        offset,
                        limit,
                    )
                )
                params.update({"offest": str(offset), "limit": str(limit)})
                rows = self._get_records(endpoint, params)
                if not rows:
                    break
                keys = rows.keys()
                for key in keys:
                    if key not in ["result_code", "result_message", "result_output"]:
                        row = rows[key]
                        if row:
                            # if we have not reached the last ts (previous execution)
                            timestamp = row.get("tstamp", None)
                            if not timestamp:
                                # for link_list tstamp resides in info
                                info = row["info"]
                                if info:
                                    timestamp = info[0]["tstamp"]
                                else:
                                    timestamp = self._default_start_time
                            record_time: datetime = datetime.strptime(
                                timestamp, self._datetime_format
                            )
                            # update the variable with highest timestamp
                            if record_time > high_ts:
                                high_ts = record_time
                            if record_time <= last_watermark:
                                break
                            # append campaign_id and message_id
                            row.update(
                                {
                                    self._campaign_id: campaign[self._campaign_id],
                                    self._message_id: campaign[self._message_id],
                                }
                            )
                            records.append(row)
                # if number of records is below offset - we have reached the end
                if len(keys) < limit:
                    break
                offset += limit
            # save the campaign result
            if records:
                self._save(context, records)
            if high_ts > last_watermark:
                self._save_last_ts(
                    campaign[self._campaign_id],
                    datetime.strftime(high_ts, self._datetime_format),
                )

    def _save_last_ts(self, campaign_id: str, last_ts: str) -> None:
        campaign_last_ts = self._active_campaign_last_ts.get(campaign_id, {})
        campaign_last_ts.update({self._api_action: last_ts})
        self._active_campaign_last_ts.update({campaign_id: campaign_last_ts})
        os.getenv("active_campaign_last_ts", self._active_campaign_last_ts)
        print(
            "setting active_campaign_last_ts -> {}".format(
                json.dumps(self._active_campaign_last_ts)
            )
        )


if __name__ == "__main__":
    context = {
        "execution_date": DateTime.now(),
        "campaign_ids": json.loads(os.getenv("CAMPAIGN_IDS")),
    }
    active_campaign_tasks = json.loads(os.getenv("ACTIVE_CAMPAIGN_TASKS"))
    active_campaign_tasks.pop(0)
    for task in active_campaign_tasks:
        task_id = task["api_action"]
        table = task_id
        ac_api = ActiveCampaignDeltaOperator(
            task_id=f"{task_id}_api",
            api_prefix=task["api_prefix"],
            data_point=task.get("data_point", None),
            api_action=task.get("api_action", None),
            sort_key=task.get("sort_key", None),
            do_xcom_push=task.get("xcom_push", False),
        )
        ac_api.execute(context)
