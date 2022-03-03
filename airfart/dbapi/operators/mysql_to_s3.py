from typing import Any

from airfart.dbapi.hooks.mysql_extended import MySqlExtendedHook
from airfart.model.output_format import OutputFormat
from airfart.operators.base_data_to_s3 import BaseDataToS3Operator


class MySQLToS3Operator(BaseDataToS3Operator):
    def __init__(self,
                 *,
                 sql: str,
                 bucket: str,
                 database: str,
                 file_name: str,
                 db_conn_id: str,
                 output_format: str = OutputFormat.JSON,
                 include_csv_headers: bool = True,
                 post_db_path: str = None,
                 records_transform_fn: callable = None,
                 session_variables: dict = None,
                 **kwargs
                 ) -> None:
        super(MySQLToS3Operator, self).__init__(sql=sql,
                                                bucket=bucket,
                                                database=database,
                                                file_name=file_name,
                                                db_conn_id=db_conn_id,
                                                output_format=output_format,
                                                include_csv_headers=include_csv_headers,
                                                post_db_path=post_db_path,
                                                records_transform_fn=records_transform_fn,
                                                **kwargs)
        self.session_variables = session_variables

    def get_hook(self):
        return MySqlExtendedHook(mysql_conn_id=self.db_conn_id,
                                 session_variables=self.session_variables)

    def execute(self, context: Any):
        super().execute(context)
