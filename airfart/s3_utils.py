import json
from typing import Dict, Optional

import smart_open


def get_last_execution(bucket: str, prefix: str, key: str = None) -> str:
    rows = []
    with smart_open.smart_open(f's3://{bucket}/{prefix}/last_execution',
                               'r') as s3_file:
        for line in s3_file:
            rows.append(line)
    contents = ''.join(rows)
    if key:
        d: Dict[str, str] = json.loads(contents)
        return d[key]
    return contents


# noinspection PyBroadException
def set_last_execution(bucket: str, prefix: str, key: str, value: str):
    try:
        raw_data: str = get_last_execution(bucket=bucket, prefix=prefix)
    except Exception:
        raw_data: Optional[str] = None

    if raw_data:
        data: Dict[str, str] = json.loads(raw_data)
    else:
        data = dict()

    data.update({key: value})
    json_data = json.dumps(data)

    with smart_open.smart_open(f's3://{bucket}/{prefix}/last_execution', 'wb') as s3_file:
        s3_file.write(json_data.encode('utf8'))
