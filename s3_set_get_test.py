import json
import os

from dotenv import load_dotenv

from airfart.s3_get_last_execution import S3GetLastExecutionOperator
from airfart.s3_set_last_execution import S3SetLastExecutionOperator

if __name__ == "__main__":
    load_dotenv()
    const_key = 'rotem'
    const_value = 'the king'
    d = json.dumps({const_key: const_value})
    bucket = os.getenv('POC_BUCKET')
    database = os.getenv('POC_DATABASE')
    model = os.getenv('POC_MODEL')
    S3SetLastExecutionOperator(bucket=bucket, database=database, model=model, key=const_key,
                               value=const_value).execute()
    value = S3GetLastExecutionOperator(bucket=bucket, database=database, model=model, key=const_key).execute()
    assert (value == const_value)
    value = S3GetLastExecutionOperator(bucket=bucket, database=database, model=model).execute()
    assert (value == d)
