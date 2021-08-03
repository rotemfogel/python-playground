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
    :param database: the s3 database. (templated)
    :type database: str
    :param model: the table name. (templated)
    :type model: str
    :param key: the data to write (e.g. partition name). (templated)
    :type key: str
    :param value: the data to write (e.g. '2021-01-01/01'). (templated)
    :type value: str
    """

    def __init__(self,
                 bucket: str,
                 database: str,
                 model: str,
                 key: str,
                 value: str):
        super().__init__()
        self.bucket = bucket
        self.database = database
        self.model = model
        self.key = key
        self.value = value

    # noinspection PyBroadException
    def execute(self):
        try:
            raw_data: str = S3GetLastExecutionOperator(bucket=self.bucket, database=self.database,
                                                       model=self.model).execute()
        except Exception:
            raw_data: Optional[str] = None

        if raw_data:
            data: Dict[str, str] = json.loads(raw_data)
        else:
            data = dict()

        data.update({self.key: self.value})
        json_data = json.dumps(data)

        try:
            with smart_open.smart_open(f's3://{self.bucket}/{self.database}/{self.model}/last_execution',
                                       'wb') as s3_file:
                s3_file.write(json_data.encode('utf8'))

            self.log.info(f"Updated {self.model} last_partition to {raw_data} in S3")

        except Exception:
            raise Exception("Error updating last_execution file")
