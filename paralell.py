import json
import os
import time
from copy import deepcopy
from multiprocessing import Pool

import requests
import smart_open
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SECRET")
params = {"api_key": api_key, "api_output": "json"}
params.update({"sort": "tstamp", "sort_direction": "DESC"})
params.update({"api_action": "campaign_report_open_list"})
endpoint = os.getenv("ENDPOINT")
campaigns = [{"campaignid": "170", "messageid": "235"}]
page = 1


def api_caller(pg, new_params=None):
    if new_params is None:
        new_params = deepcopy(params)
    local_records = []
    new_params.update(dict(page=pg + page))
    response = requests.request("GET", endpoint, params=new_params)
    print(new_params)
    if response:
        json_response = response.json()
        for key in json_response.keys():
            if key not in ["result_code", "result_message", "result_output"]:
                row = json_response[key]
                row.update({"campaignid": "170", "messageid": "235"})
                local_records.append(row)
    return local_records


if __name__ == "__main__":
    total_records = []
    for campaign in campaigns:
        params.update(
            {"campaignid": campaign["campaignid"], "messageid": campaign["messageid"]}
        )
        while page < 20:
            with Pool(processes=5) as pool:
                result = pool.map(api_caller, [0, 1, 2, 3, 4], chunksize=5)
                total_records.append(result)
            page += 5
            time.sleep(1)
            print(page)
    #
    uri = f"5_campaign_report_open_list_campaignid_170_messageid_235.json"
    with smart_open.open(uri=uri, mode="wb") as s3_file:
        for records in total_records:
            if not records:
                continue
            for record in records:
                s3_file.write((json.dumps(record) + "\n").encode())
