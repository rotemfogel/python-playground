import json
import logging
from enum import IntEnum

import pandas as pd
from airflow.exceptions import AirflowException
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf import json_format
from pandas import DataFrame
from typing import Optional

from airfart.base_hook import BaseHook


class OutputFormat(object):
    JSON: str = 'json'
    PARQUET: str = 'parquet'
    CSV: str = 'csv'


class GoogleAdsApiType(IntEnum):
    SearchStream = 1
    Search = 2


class GoogleAdsApiHook(BaseHook):

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

    def get_conn(self):
        if self.client is None:
            # credentials = Variable.get('google_ads_credentials', deserialize_json=True)
            # self.client = GoogleAdsClient.load_from_dict(credentials, version="v9")
            self.client = GoogleAdsClient.load_from_storage(version="v9")
            self.customer_id = self.client.login_customer_id
            self.service = self.client.get_service("GoogleAdsService", version="v9")
        return self.service

    def _get_records(self, sql):
        results = None
        if self.method == GoogleAdsApiType.SearchStream:
            results = self._search_stream(sql)
        if self.method == GoogleAdsApiType.Search:
            results = self._search(sql)
        if results:
            return pd.DataFrame(results)
        raise AirflowException('Unknown GoogleAdsApiType: {}'.format(self.method))

    def get_records(self, sql):
        return self._get_records(sql)
