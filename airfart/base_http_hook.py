import json
import os

from airfart.base_hook import BaseHook
from airfart.connection import Connection


class BaseHttpHook(BaseHook):
    def __init__(self, conn_id: str = None) -> None:
        super().__init__(self, conn_id=conn_id)
        self._active_conf: str = os.getenv("ACTIVE_CAMPAIGN_CONF")
        self._active_campaign_conf: dict = json.loads(self._active_conf)

        self._connection = Connection(
            conn_id="active_campaign",
            host=self._active_campaign_conf["endpoint"],
            extra={"api_key": self._active_campaign_conf["secret"]},
        )

    def get_conn(self):
        return self._connection
