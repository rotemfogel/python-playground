import os
import urllib

import requests
from dotenv import load_dotenv, find_dotenv

loaded = load_dotenv(find_dotenv())

if loaded:
    endpoint = os.getenv('ENDPOINT')
    secret = os.getenv('SECRET')
    api_action = os.getenv('API_ACTION')
    full = bool(os.getenv('FULL'))
    full = '1' if full else '0'

    page: int = 1
    while True:
        params = {'api_action': api_action, 'api_key': secret, 'full': full, 'api_output': 'json', 'page': str(page)}
        query_params = '&'.join(
            list(
                map(lambda kv: ("{}={}".format(urllib.parse.quote(kv[0]), urllib.parse.quote(kv[1]))), params.items())))
        endpoint = '?'.join([endpoint, query_params])
        response = requests.request("GET", endpoint, data={})
        response = response.json()
        if response['result_code'] == 0:
            break
        # write response
        page += 1
