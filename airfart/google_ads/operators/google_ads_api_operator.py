from datetime import timedelta

from airfart.google_ads.hooks.google_ads_api_hook import GoogleAdsApiType, OutputFormat, GoogleAdsApiHook
from airfart.google_ads.operators.base_data_to_s3_operator import BaseDataToS3Operator


class GoogleAdsApiOperator(BaseDataToS3Operator):

    def __init__(self,
                 sql: str,
                 bucket: str,
                 database: str,
                 table: str,
                 customer_id: str,
                 method: GoogleAdsApiType = GoogleAdsApiType.Search,
                 output_format: str = OutputFormat.PARQUET,
                 execution_timeout: timedelta = timedelta(minutes=5),
                 *args, **kwargs):
        super().__init__(sql=sql,
                         bucket=bucket,
                         database=database,
                         file_name=table,
                         output_format=output_format,
                         post_db_path=f'{table}/account_id={customer_id}',
                         execution_timeout=execution_timeout,
                         *args, **kwargs)
        self.method = method
        self.hook = None
        self.customer_id = customer_id

    def get_hook(self):
        if not self.hook:
            self.hook = GoogleAdsApiHook(method=self.method, customer_id=self.customer_id)
        return self.hook

    def execute(self):
        return super().execute()
