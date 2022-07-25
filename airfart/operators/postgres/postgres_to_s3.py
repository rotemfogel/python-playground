from datetime import timedelta

from airfart.hooks.postgres.postgres_extended import PostgresExtendedHook
from airfart.model.output_format import OutputFormat
from airfart.operators.base_data_to_s3 import BaseDataToS3Operator


class PostgresToS3Operator(BaseDataToS3Operator):
    def __init__(self,
                 sql: str,
                 bucket: str,
                 database: str,
                 file_name: str,
                 db_conn_id: str,
                 output_format: str = OutputFormat.PARQUET,
                 include_csv_headers: bool = True,
                 post_db_path: str = None,
                 records_transform_fn: callable = None,
                 execution_timeout: timedelta = timedelta(minutes=5),
                 **kwargs
                 ) -> None:
        super(PostgresToS3Operator, self).__init__(sql=sql,
                                                   bucket=bucket,
                                                   database=database,
                                                   file_name=file_name,
                                                   db_conn_id=db_conn_id,
                                                   output_format=output_format,
                                                   include_csv_headers=include_csv_headers,
                                                   post_db_path=post_db_path,
                                                   records_transform_fn=records_transform_fn,
                                                   execution_timeout=execution_timeout,
                                                   **kwargs
                                                   )

    def get_hook(self):
        return PostgresExtendedHook()
