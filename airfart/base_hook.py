from airflow.utils.log.logging_mixin import LoggingMixin


class BaseHook(LoggingMixin):

    def __init__(self,
                 conn_id: str = None) -> None:
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self):
        raise NotImplementedError
