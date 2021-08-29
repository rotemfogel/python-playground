import os

from dotenv import load_dotenv
from typing import List

from airfart.s3_list_keys_operator import S3ListKeysOperator
from airfart.s3_utils import set_last_execution, get_last_execution

if __name__ == "__main__":
    load_dotenv()
    bucket = os.getenv('POC_BUCKET')
    database = os.getenv('POC_DATABASE')
    model = os.getenv('POC_MODEL')
    prefix = f'{database}/{model}'
    keys: List[str] = S3ListKeysOperator().list_keys(bucket, prefix)
    set_last_execution(bucket, prefix, 'prepared_data', keys[-1].replace(prefix, ''))
    value = get_last_execution(bucket, prefix, 'prepared_data')
    print(value)
