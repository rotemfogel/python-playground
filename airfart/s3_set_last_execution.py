import json
from typing import Dict, Optional

import smart_open
from airflow.utils.log.logging_mixin import LoggingMixin

from airfart.s3_get_last_execution import S3GetLastExecutionOperator


class S3SetLastExecutionOperator(LoggingMixin):
    """
    This operator writes a file that marks the latest partition of an athena table.
    used by sapi for loading data

    :param bucket: The S3 bucket where to find the objects. (templated)
    :type bucket: str
    :param prefix: the s3 database. (templated)
    :type prefix: str
    :param key: the data to write (e.g. partition name). (templated)
    :type key: str
    :param value: the data to write (e.g. '2021-01-01/01'). (templated)
    :type value: str
    """

    def __init__(self,
                 bucket: str,
                 prefix: str,
                 key: str,
                 value: str):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix
        self.key = key
        self.value = value

    # noinspection PyBroadException
    def execute(self):
        try:
            raw_data: str = S3GetLastExecutionOperator(bucket=self.bucket, prefix=self.prefix).execute()
        except Exception:
            raw_data: Optional[str] = None

        if raw_data:
            data: Dict[str, str] = json.loads(raw_data)
        else:
            data = dict()

        data.update({self.key: self.value})
        json_data = json.dumps(data)

        try:
            with smart_open.smart_open(f's3://{self.bucket}/{self.prefix}/last_execution',
                                       'wb') as s3_file:
                s3_file.write(json_data.encode('utf8'))

            self.log.info(f"Updated {self.prefix} last_partition to {raw_data} in S3")

        except Exception:
            raise Exception("Error updating last_execution file")
