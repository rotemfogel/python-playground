import os

from dotenv import load_dotenv

from airfart.hooks.db.db import DBHook
from airfart.model.db import DB, DBType


class PostgresExtendedHook(DBHook):

    def __init__(self) -> None:
        super().__init__()
        load_dotenv()
        self.db = DB(DBType.PostgreSQL,
                     os.getenv('POSTGRES_LOGIN'), os.getenv('POSTGRES_PASSWORD'),
                     os.getenv('POSTGRES_SCHEMA'), int(os.getenv('POSTGRES_PORT')),
                     os.getenv('POSTGRES_HOST'))


