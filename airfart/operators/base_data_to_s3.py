import json
from abc import ABC
from datetime import timedelta
from typing import Optional

import smart_open
from airflow.utils.log.logging_mixin import LoggingMixin

from airfart.model.output_format import OutputFormat


class BaseDataToS3Operator(LoggingMixin, ABC):
    """
    The following Operator submits a query to a Postgres Client,
     the results of the query are copied to an S3 location

    Currently, only supporting Gzip output format
    :param sql: query to execute. (Templated)
    :type sql: str
    :param bucket: The S3 bucket where to find the objects. (Templated)
    :type bucket: str
    :param database: Database to select. (Templated)
    :type database: str
    :param post_db_path: location specification. i.e. s://bucket/database/[post_db_path]/file_name.json.gz (Templated)
    :type post_db_path
    :param file_name: (Templated)
    :type file_name: str
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
        Default is set to 5 minutes. (based on max of 6s, of 12 runs)
    :type execution_timeout: datetime.timedelta

    """
    __ALLOWED_FORMATS = [OutputFormat.JSON, OutputFormat.PARQUET, OutputFormat.CSV]
    log = LoggingMixin.log

    def __init__(self,
                 *,
                 sql: str,
                 bucket: str,
                 database: str,
                 file_name: str,
                 db_conn_id: Optional[str] = None,
                 post_db_path: Optional[str] = None,
                 output_format: str = OutputFormat.JSON,
                 include_csv_headers: bool = True,
                 records_transform_fn: callable = None,
                 execution_timeout: timedelta = timedelta(minutes=5),
                 **kwargs):
        super().__init__()
        assert output_format in self.__ALLOWED_FORMATS, \
            f'output_format should be either {OutputFormat.JSON}, {OutputFormat.PARQUET} or {OutputFormat.CSV}! '

        self.sql = sql
        self.bucket = bucket
        self.database = database
        self.file_name = file_name
        self.db_conn_id = db_conn_id
        self.post_db_path = post_db_path
        self.output_format = output_format
        self.include_csv_headers = include_csv_headers
        self.records_transform_fn = records_transform_fn
        self.execution_timeout = execution_timeout

    def get_hook(self):
        raise NotImplementedError

    def get_pandas_df(self):
        """
        get records from the hook
        :return: list of records
        """
        return self.get_hook().get_pandas_df(self.sql)

    def get_records(self):
        """
        get records from the hook
        :return: list of records
        """
        return self.get_hook().get_records(self.sql)

    def execute(self):
        # Generate write destination
        path_components = ['s3:/', self.bucket, self.database, self.post_db_path, self.file_name]
        path_components = [p_c for p_c in path_components if p_c]  # removing None segments
        if self.output_format == OutputFormat.JSON:
            suffix = '.json.gz'
        elif self.output_format == OutputFormat.CSV:
            suffix = '.csv'
        else:  # OutputFormat.PARQUET
            suffix = '.parquet'
        uri = '/'.join(path_components) + suffix
        self.log.debug(f"\nGenerated destination: {uri}\n")

        # Query logging
        self.log.debug('\nExecuting the following query: %s\n', self.sql)

        df = self.get_pandas_df()

        if self.output_format == OutputFormat.PARQUET:
            if self.records_transform_fn:
                df = self.records_transform_fn(df)
            df.to_parquet(uri, engine='pyarrow', allow_truncated_timestamps=True)
        else:
            if self.output_format == OutputFormat.JSON:
                columns = df.select_dtypes(include=['datetime64']).columns
                for column in columns:
                    df[column] = df[column].astype(str)
                records = df.to_dict(orient="records")
            else:  # self.CSV
                records = df.to_csv(index=False, header=self.include_csv_headers).split("\n")

            # Copy to S3
            self.log.info(f'about to write to file {uri}')
            with smart_open.smart_open(uri, 'wb') as s3_file:
                for record in records:
                    if not record:
                        continue
                    record_to_write = self.records_transform_fn(record) if self.records_transform_fn else record
                    s3_file.write((json.dumps(record_to_write, default=lambda o: o.__dict__) + '\n').encode('utf8'))

        self.log.info('All done')
