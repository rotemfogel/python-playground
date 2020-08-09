from dotenv import load_dotenv, find_dotenv

from airfart.append_http_hook import AppendHttpHook

load_dotenv(find_dotenv())
hook = AppendHttpHook(http_conn_id='active_campaign')
result = hook.run(endpoint='/api/3/campaigns', data={'api_output': 'json'})
print(result)
