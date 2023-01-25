import urllib

import urllib3

from airfart.base_http_hook import BaseHttpHook
from airfart.connection import Connection
from airfart.http_hook import HttpHook


class AppendHttpHook(BaseHttpHook):
    def __init__(self, method: str = "GET", http_conn_id: str = "http_default") -> None:
        super().__init__()
        self._method: str = method
        self._connection: Connection = self.get_connection(http_conn_id)
        self._host: str = self._connection.host
        self._extra_options: dict = self._connection.extra
        self._hook = HttpHook(method, http_conn_id)
        urllib3.disable_warnings()

    def get_conn(self):
        return self._hook

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, endpoint: str = None, data: dict = None, headers: dict = None):

        if data:
            if self._extra_options:
                for k, v in data.items():
                    self._extra_options.update({k: str(v)})
            else:
                self._extra_options = data

        uri = "?" + urllib.parse.urlencode(self._extra_options)
        if endpoint:
            endpoint = endpoint + uri
        else:
            endpoint = uri

        self.get_conn().run(endpoint=endpoint, data=data, headers=headers)
