import os
from contextlib import closing
from typing import Optional

from dotenv import load_dotenv
from mysql.connector import connect
from pandas.io import sql as psql


class DB():
    def __init__(self,
                 login: str,
                 password: str,
                 schema: Optional[str] = None,
                 port: Optional[int] = 3306,
                 host: Optional[str] = '127.0.0.1'):
        super().__init__()
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.host = host


class MySqlExtendedHook():
    __session_variables: str = 'session_variables'
    __default_session_variables: dict = dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        if self.__session_variables not in kwargs:
            setattr(self, self.__session_variables, self.__default_session_variables)
        else:
            setattr(self, self.__session_variables, dict(kwargs[self.__session_variables]))
        load_dotenv()
        self.db = DB(os.getenv('MYSQL_LOGIN'), os.getenv('MYSQL_PASSWORD'),
                     os.getenv('MYSQL_SCHEMA'), int(os.getenv('MYSQL_PORT')),
                     os.getenv('MYSQL_HOST'))

    def get_conn(self):
        """
        create a database connection from scratch every call to get_conn()
        since MariaDB fails with error MySQLdb._exceptions.OperationalError: (2006, '')
        on large queries
        """
        conn = connect(user=self.db.login,
                       password=self.db.password,
                       host=self.db.host or '127.0.0.1',
                       port=self.db.port,
                       database=self.db.schema)
        # Apply session variables if needed
        session_variables = getattr(self, self.__session_variables)
        if session_variables:
            with closing(conn.cursor()) as cur:
                for k, v in session_variables.items():
                    set_session = f'SET SESSION {k}={v}'
                    cur.execute(set_session)
        return conn

    def get_pandas_df(self, sql, parameters=None, **kwargs):
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        :param kwargs: (optional) passed into pandas.io.sql.read_sql method
        :type kwargs: dict
        """
        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters, **kwargs)

    def get_records(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchall()
