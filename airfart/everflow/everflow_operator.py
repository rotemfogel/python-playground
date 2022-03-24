from time import sleep
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
        self.skip_send = True

    def execute(self):
        if self.records is not None and len(self.records) > 0:
            from dotenv import load_dotenv
            import json
            import os
            load_dotenv()
            everflow_config: Dict[str, Any] = json.loads(os.getenv('everflow_api_config'))
            query_params = dict(verification_token=everflow_config['verification_token'],
                                aid=everflow_config['advid'],
                                nid=everflow_config['nid'])

            failures = []
            event_config = everflow_config['event_id']
            everflow_records: List[EverFlowRecord] = to_everflow_record(self.records)
            for record in everflow_records:
                product = record.product
                subscription_type = record.subscription_type
                rate_plan = record.rate_plan
                query_params.update(dict(gclid=str(record.user_id),
                                         adv_event_id=event_config[product][subscription_type][rate_plan]))
                status_code = 0
                retry: int = 0
                if not self.skip_send:
                    while retry < self.retries and status_code not in (200, 204):
                        response = requests.get(url=everflow_config["domain"], params=query_params)
                        status_code = response.status_code
                        retry += 1
                        if status_code not in (200, 204):
                            sleep(1000)
                    if retry >= self.retries and status_code not in (200, 204):
                        failures.append(record)

            if failures:
                raise AirflowException(f"operator failed to send {len(self.records)} "
                                       f"out of {len(self.records)}\nfailed records: {failures}")
        print("all done")
