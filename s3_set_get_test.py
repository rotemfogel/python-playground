import json
import os

from dotenv import load_dotenv

from airfart.s3_utils import set_last_execution, get_last_execution

if __name__ == "__main__":
    load_dotenv()
    const_key = "rotem"
    const_value = "the king"
    d = json.dumps({const_key: const_value})
    bucket = os.getenv("POC_BUCKET")
    database = os.getenv("POC_DATABASE")
    model = os.getenv("POC_MODEL")
    prefix = f"{database}/{model}"
    set_last_execution(bucket=bucket, prefix=prefix, key=const_key, value=const_value)
    value = get_last_execution(bucket=bucket, prefix=prefix, key=const_key)
    assert value == const_value
    value = get_last_execution(bucket=bucket, prefix=prefix)
    assert value == d
