import os
from contextlib import closing

from dotenv import load_dotenv

from airfart.hooks.db.db import DBHook
from airfart.model.db import DB, DBType


class MySqlExtendedHook(DBHook):
    __session_variables: str = "session_variables"
    __default_session_variables: dict = dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        if self.__session_variables not in kwargs:
            setattr(self, self.__session_variables, self.__default_session_variables)
        else:
            setattr(
                self, self.__session_variables, dict(kwargs[self.__session_variables])
            )
        load_dotenv()
        self.db = DB(
            DBType.MySQL,
            os.getenv("MYSQL_LOGIN"),
            os.getenv("MYSQL_PASSWORD"),
            os.getenv("MYSQL_SCHEMA"),
            int(os.getenv("MYSQL_PORT")),
            os.getenv("MYSQL_HOST"),
        )

    def get_conn(self):
        """
        create a database connection from scratch every call to get_conn()
        since MariaDB fails with error MySQLdb._exceptions.OperationalError: (2006, '')
        on large queries
        """
        conn = super().get_conn()
        # Apply session variables if needed
        session_variables = getattr(self, self.__session_variables)
        if session_variables:
            with closing(conn.cursor()) as cur:
                for k, v in session_variables.items():
                    set_session = f"SET SESSION {k}={v}"
                    cur.execute(set_session)
        return conn
