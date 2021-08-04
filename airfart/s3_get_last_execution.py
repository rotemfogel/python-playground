import json
from typing import Optional, Dict

import smart_open
from airflow.utils.log.logging_mixin import LoggingMixin


class S3GetLastExecutionOperator(LoggingMixin):
    """
    This operator writes a file that marks the latest partition of an athena table.
    used by sapi for loading data

    :param bucket: The S3 bucket where to find the objects. (templated)
    :type bucket: str
    :param prefix: the s3 database. (templated)
    :type prefix: str
    :param key: the key to write (e.g. training). (templated)
    :type key: str
    """

    def __init__(self, bucket: str, prefix: str, key: Optional[str] = None):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix
        self.key = key

    def execute(self):
        try:
            rows = []
            with smart_open.smart_open(f's3://{self.bucket}/{self.prefix}/last_execution',
                                       'r') as s3_file:
                for line in s3_file:
                    rows.append(line)
            contents = ''.join(rows)
            if self.key:
                d: Dict[str, str] = json.loads(contents)
                return d[self.key]
            return contents
        except Exception as e:
            raise Exception(f"Error reading last_execution file: {str(e)}")
