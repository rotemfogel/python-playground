import json
import os

from airfart.connection import Connection


class BaseHook:
    def __init__(self) -> None:
        super().__init__()
        self._active_conf: str = os.getenv('ACTIVE_CAMPAIGN_CONF')
        self._active_campaign_conf: dict = json.loads(self._active_conf)

        self._connection = Connection(conn_id='active_campaign',
                                      host=self._active_campaign_conf['endpoint'],
                                      extra={'api_key': self._active_campaign_conf['secret']})

    def get_connection(self, http_conn_id: str) -> Connection:
        return self._connection
