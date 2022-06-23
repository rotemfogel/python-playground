import json
import logging
import os
from typing import Optional

import pandas as pd
from airflow.exceptions import AirflowException
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf import json_format

from airfart.base_hook import BaseHook
from airfart.model.google_ads.google_ads_api_type import GoogleAdsApiType

load_dotenv()


class GoogleAdsApiHook(BaseHook):
    __ALLOWED_API_TYPES = [GoogleAdsApiType.Search, GoogleAdsApiType.SearchStream]
    __GOOGLE_ADS_API_VERSION = 'v9'

    __SEARCH_GOOGLE_ADS_SERVICE = 'SearchGoogleAdsRequest'
    __STREAM_GOOGLE_ADS_SERVICE = 'GoogleAdsService'

    def __init__(self,
                 api_type: GoogleAdsApiType,
                 account_id: str,
                 logging_level: int = logging.INFO) -> None:
        super().__init__()
        if api_type not in self.__ALLOWED_API_TYPES:
            raise AirflowException(f'Unknown GoogleAdsApiType: {api_type}')
        self.api_type = api_type
        self.account_id = account_id
        self.__client = None
        self.__search_service = None
        self.__stream_service = None
        # noinspection SpellCheckingInspection
        logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] %(message).5000s')
        logging.getLogger('google.ads.googleads.client').setLevel(logging_level)

    def get_conn(self):
        if self.__client is None:
            credentials = json.loads(os.getenv('google_ads_credentials'))
            self.__client = GoogleAdsClient.load_from_dict(credentials, version="v11")
        return self.__client

    def _get_search_service(self):
        if not self.__search_service:
            self.__search_service = self.get_conn().get_service(self.__SEARCH_GOOGLE_ADS_SERVICE,
                                                                version=self.__GOOGLE_ADS_API_VERSION)
        return self.__search_service

    def _get_stream_service(self):
        if not self.__stream_service:
            self.__stream_service = self.get_conn().get_service(self.__STREAM_GOOGLE_ADS_SERVICE,
                                                                version=self.__GOOGLE_ADS_API_VERSION)
        return self.__stream_service

    def _search(self, sql) -> Optional[list]:
        """
        search api
        """
        request = self._get_search_service()
        request.account_id = self.account_id
        request.query = sql
        results = []
        # emulate do-while
        response_proto = self.get_conn().search(request)
        for page in response_proto:
            json_str = json_format.MessageToJson(page)
            response = json.loads(json_str)
            results.append(response)
        return results

    def _search_stream(self, sql) -> Optional[list]:
        """
        stream api
        """
        stream_result = self._get_stream_service().search_stream(customer_id=self.account_id, query=sql)
        results = []
        for batch in stream_result:
            for row in batch.results:
                json_str = json_format.MessageToJson(row)
                obj = json.loads(json_str)
                results.append(obj)
        return results

    def get_records(self, sql):
        if self.api_type == GoogleAdsApiType.SearchStream:
            return self._search_stream(sql)
        if self.api_type == GoogleAdsApiType.Search:
            return self._search(sql)
        return list()

    def get_pandas_df(self, sql):
        return pd.DataFrame(self.get_records(sql))
