from typing import Optional

from airflow.utils.log.logging_mixin import LoggingMixin

from airfart.s3_utils import get_last_execution


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
            return get_last_execution(self.bucket, self.prefix, self.key)
        except Exception as e:
            raise Exception(f"Error reading last_execution file: {str(e)}")
