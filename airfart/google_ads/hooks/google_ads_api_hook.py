import json
import logging
import os
from typing import Optional

import pandas as pd
from airflow.exceptions import AirflowException
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf import json_format
from pandas import DataFrame

from airfart.base_hook import BaseHook
from airfart.google_ads.model.google_ads_api_type import GoogleAdsApiType

load_dotenv()


class GoogleAdsApiHook(BaseHook):
    __ALLOWED_METHODS = [GoogleAdsApiType.Search, GoogleAdsApiType.SearchStream]

    def __init__(self,
                 method: GoogleAdsApiType,
                 customer_id: str,
                 logging_level: int = logging.INFO):
        super().__init__(conn_id=None)
        if not method in self.__ALLOWED_METHODS:
            raise AirflowException('Unknown GoogleAdsApiType: {}'.format(self.method))
        self.method = method
        self.client = None
        self.customer_id = customer_id
        self.client = None
        self.service = None
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
        response_proto = self.get_conn().search(request)
        for page in response_proto:
            json_str = json_format.MessageToJson(page)
            response = json.loads(json_str)
            results.append(response)
        return results

    def _search_stream(self, sql) -> Optional[DataFrame]:
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

    def _get_records(self, sql):
        results = None
        if self.method == GoogleAdsApiType.SearchStream:
            results = self._search_stream(sql)
        if self.method == GoogleAdsApiType.Search:
            results = self._search(sql)
        if results:
            return pd.DataFrame(results)

    def get_records(self, sql):
        return self._get_records(sql)
