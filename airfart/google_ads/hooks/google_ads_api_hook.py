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
from airfart.google_ads.model.google_ads_api_type import GoogleAdsApiType

load_dotenv()


class GoogleAdsApiHook(BaseHook):
    __ALLOWED_API_TYPES = [GoogleAdsApiType.Search, GoogleAdsApiType.SearchStream]
    __GOOGLE_ADS_API_VERSION = "v9"

    def __init__(self,
                 api_type: GoogleAdsApiType,
                 customer_id: str,
                 logging_level: int = logging.INFO) -> None:
        super().__init__()
        if api_type not in self.__ALLOWED_API_TYPES:
            raise AirflowException(f'Unknown GoogleAdsApiType: {api_type}')
        self.api_type = api_type
        self.customer_id = customer_id
        self.client = None
        self.service = None
        # noinspection SpellCheckingInspection
        logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] %(message).5000s')
        logging.getLogger('google.ads.googleads.client').setLevel(logging_level)

    def get_conn(self):
        if self.client is None:
            # credentials = Variable.get('google_ads_credentials', deserialize_json=True)
            credentials = json.loads(os.getenv('google_ads_credentials'))
            self.client = GoogleAdsClient.load_from_dict(credentials, version="v9")
            # self.client = GoogleAdsClient.load_from_storage(version="v9")
            self.service = self.client.get_service("GoogleAdsService", version="v9")
        return self.service

    def _search(self, sql) -> Optional[list]:
        """
        search api
        """
        self.get_conn()
        request = self.client.get_type('SearchGoogleAdsRequest')
        request.customer_id = self.customer_id
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
        stream = self.get_conn().search_stream(customer_id=self.customer_id, query=sql)
        results = []
        for batch in stream:
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
        raise AirflowException('Invalid api_type provided')

    def get_pandas_df(self, sql):
        return pd.DataFrame(self.get_records(sql))
