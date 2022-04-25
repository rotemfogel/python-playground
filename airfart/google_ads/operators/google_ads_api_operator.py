from datetime import timedelta

from airfart.google_ads.hooks.google_ads_api_hook import GoogleAdsApiType, GoogleAdsApiHook
from airfart.model.output_format import OutputFormat
from airfart.operators.base_data_to_s3 import BaseDataToS3Operator


class GoogleAdsApiOperator(BaseDataToS3Operator):

    def __init__(self,
                 *,
                 sql: str,
                 bucket: str,
                 database: str,
                 table: str,
                 account_id: str,
                 post_db_path: str = None,
                 method: GoogleAdsApiType = GoogleAdsApiType.Search,
                 output_format: str = OutputFormat.PARQUET,
                 records_transform_fn: callable = None,
                 execution_timeout: timedelta = timedelta(minutes=5),
                 **kwargs):
        super().__init__(sql=sql,
                         bucket=bucket,
                         database=database,
                         file_name=table,
                         output_format=output_format,
                         post_db_path=post_db_path,
                         records_transform_fn=records_transform_fn,
                         execution_timeout=execution_timeout,
                         **kwargs)
        self.method = method
        self.account_id = account_id
        self.__hook = None

    def get_hook(self):
        if not self.__hook:
            self.__hook = GoogleAdsApiHook(api_type=self.method, account_id=self.account_id)
        return self.__hook

    def execute(self):
        return super().execute()
