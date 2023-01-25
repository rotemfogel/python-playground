from abc import ABC
from contextlib import closing
from typing import Optional

import MySQLdb
import psycopg2

from airfart.model.db import DB, DBType


class DBHook(ABC):
    def __init__(self, db: Optional[DB] = None):
        self.db = db

    def get_conn(self):
        """
        create a database connection from scratch every call to get_conn()
        since MariaDB fails with error POSTGRESdb._exceptions.OperationalError: (2006, '')
        on large queries
        """
        conn_args = dict(
            dbname=self.db.schema,
            user=self.db.login,
            password=self.db.password,
            host=self.db.host,
            port=self.db.port,
        )
        if self.db.db_type == DBType.MySQL:
            return MySQLdb.connect(**conn_args)
        return psycopg2.connect(**conn_args)

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

        try:
            from pandas.io import sql as psql
        except ImportError:
            raise Exception(
                "pandas library not installed, run: pip install "
                "'apache-airflow-providers-common-sql[pandas]'."
            )

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
