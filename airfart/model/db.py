from enum import IntEnum
from typing import Optional


class DBType(IntEnum):
    MySQL = 1
    PostgreSQL = 2


class DB:
    def __init__(
        self,
        db_type: DBType,
        login: str,
        password: str,
        schema: Optional[str] = None,
        port: Optional[int] = None,
        host: Optional[str] = None,
    ):
        super().__init__()
        self.db_type = db_type
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port or 5432 if db_type == DBType.PostgreSQL else 3306
        self.host = host or "127.0.0.1"
