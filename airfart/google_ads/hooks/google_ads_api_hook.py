from enum import IntEnum
from typing import Optional

from airflow.exceptions import AirflowException
from google.ads.googleads.client import GoogleAdsClient
from pandas import DataFrame

from airfart.base_hook import BaseHook
import logging

class OutputFormat(object):
    JSON: str = 'json'
    PARQUET: str = 'parquet'
    CSV: str = 'csv'


class GoogleAdsApiType(IntEnum):
    SearchStream = 1
    Search = 2


class GoogleAdsApiHook(BaseHook):
    __GOOGLE_ADS_API_TOKEN = 'google_ads_api_tokens'
    __NEXT_TOKEN = 'next_token'
    __SCOPES = ["https://www.googleapis.com/auth/adwords",
                "https://adwords.google.com/api/adwords",
                "https://adwords.google.com/api/adwords/",
                "https://adwords.google.com/api/adwords/cm"]
    __CLIENT_SECRET_PATH = "/home/rotem/google-ads.json"

    def __init__(self,
                 method: GoogleAdsApiType):
        super().__init__(conn_id=None)
        self.method = method
        self.client = None
        self.customer_id = None
        self.client = None
        self.service = None
        logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] %(message).5000s')
        logging.getLogger('google.ads.googleads.client').setLevel(logging.DEBUG)

    def _search(self, sql) -> Optional[DataFrame]:
        """
        search api
        """
        self.get_conn()
        request = self.client.get_type('SearchGoogleAdsRequest')
        request.customer_id = self.client.login_customer_id
        request.query = sql
        results = []
        # emulate do-while
        while True:
            response = self.get_conn().search(request)
            results.append(response)
            if response.get(self.__NEXT_TOKEN):
                request.update({self.__NEXT_TOKEN: response[self.__NEXT_TOKEN]})
            else:
                break
        return results

    def _search_stream(self, sql) -> Optional[DataFrame]:
        """
        stream api
        """
        response = self.get_conn().search_stream(customer_id=self.customer_id, query=sql)
        results = []
        for element in response:
            results.append(element)
        return results

    def get_conn(self):
        if self.client is None:
            # credentials = Variable.get('google_ads_credentials', deserialize_json=True)
            # self.client = GoogleAdsClient.load_from_dict(credentials, version="v9")
            self.client = GoogleAdsClient.load_from_storage(version="v9")
            self.customer_id = self.client.login_customer_id
            self.service = self.client.get_service("GoogleAdsService", version="v9")
        return self.service

    def get_records(self, sql):
        if self.method == GoogleAdsApiType.SearchStream:
            return self._search_stream(sql)
        if self.method == GoogleAdsApiType.Search:
            return self._search(sql)
        raise AirflowException('Unknown GoogleAdsApiType: {}'.format(self.method))
