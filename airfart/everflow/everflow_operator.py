import time
from typing import List, Dict, Any, Union

import requests
from airflow.exceptions import AirflowException

from airfart.everflow.everflow import EverFlowRecord, to_everflow_record


class EverFlowOperator(object):

    def __init__(self,
                 records: Union[List[List[Any]], str]):
        super().__init__()
        self.records = records
        self.retries = 3

    def _send_http(self, url: str, query_params: Dict[str, Any]) -> int:
        status_code = 0
        retry: int = 0
        while retry < self.retries and status_code != 200:
            response = requests.get(url=url, params=query_params)
            status_code = response.status_code
            retry += 1
            if status_code != 200:
                time.sleep(1)
        return status_code

    def execute(self):
        everflow_records: List[EverFlowRecord] = to_everflow_record(self.records)
        if everflow_records and len(everflow_records) > 0:
            from dotenv import load_dotenv
            import json
            import os
            load_dotenv()
            everflow_config: Dict[str, Any] = json.loads(os.getenv('everflow_api_config'))
            url = everflow_config["domain"]
            event_query_params = dict(verification_token=everflow_config['verification_token'],
                                      advid=everflow_config['advid'],
                                      nid=everflow_config['nid'])

            event_config = everflow_config['event_id']
            failures = []
            for record in everflow_records:
                user_id = str(record.user_id)
                # send register
                register_query_params = {'gclid': user_id,
                                         'affid': record.attribution_affid,
                                         'oid': record.attribution_oid,
                                         'async': 'json'}

                status_code = self._send_http(f'{url}/sdk/click', register_query_params)
                if status_code == 200:
                    # need to wait until record is saved in Everflow systems
                    time.sleep(1)

                    # send the event
                    product = record.product
                    subscription_type = record.subscription_type
                    rate_plan = record.rate_plan

                    event_query_params.update(dict(gclid=user_id,
                                                   adv_event_id=event_config[product][subscription_type][rate_plan]))
                    status_code = self._send_http(url, event_query_params)
                    if status_code != 200:
                        failures.append(record)
                else:
                    failures.append(record)

            if failures:
                raise AirflowException(f"operator failed to send {len(self.records)} "
                                       f"out of {len(self.records)}\nfailed records: {failures}")
        print("all done")
