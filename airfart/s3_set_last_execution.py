from airflow.utils.log.logging_mixin import LoggingMixin

from airfart.s3_utils import set_last_execution


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

    def __init__(self, bucket: str, prefix: str, key: str, value: str):
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix
        self.key = key
        self.value = value

    # noinspection PyBroadException
    def execute(self):
        try:
            set_last_execution(self.bucket, self.prefix, self.key, self.value)
            self.log.info(f"Updated {self.prefix} {self.key} to {self.value} in S3")
        except Exception:
            raise Exception("Error updating last_execution file")
