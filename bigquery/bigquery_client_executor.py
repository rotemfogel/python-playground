import glob
import os
from copy import copy
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from bigquery.bigquery_config import PROJECT_NAME


@dataclass
class ConfigurationItem:
    start_date: date | datetime
    end_date: date | datetime
    query: str
    prefix: str
    concat: bool = True


def _get_file_name(prefix: str, start_date: str) -> str:
    return f"data/{prefix}_{start_date}.parquet"


def concat(prefix: str, start: date, end: date):
    files = glob.glob(f"data/{prefix}*.parquet")
    data_frames = []
    for file in files:
        data_frames.append(pd.read_parquet(file))
    df = pd.concat(data_frames)
    df.to_parquet(f"data/{prefix}_{start:%Y-%m-%d}-{end:%Y-%m-%d}.parquet")


class BigQueryClientExecutor(object):
    def __init__(self, configs: List[ConfigurationItem]):
        load_dotenv()
        self._BIGQUERY_CLIENT_EMAIL = os.getenv("GOOGLE_CLOUD_BIGQUERY_CLIENT_EMAIL")
        self._BIGQUERY_CLIENT_SECRET = os.getenv("GOOGLE_CLOUD_BIGQUERY_CLIENT_SECRET")
        self._GOOGLE_CREDENTIALS = service_account.Credentials.from_service_account_info(
            {
                "type": "service_account",
                "project_id": PROJECT_NAME,
                "private_key": self._BIGQUERY_CLIENT_SECRET.replace("\\n", "\n"),
                "client_email": self._BIGQUERY_CLIENT_EMAIL,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token",
            },
        )
        self._BIGQUERY_CLIENT = bigquery.Client(
            credentials=self._GOOGLE_CREDENTIALS, project=PROJECT_NAME
        )
        self._configs = configs

    def _get_data(self,
                  start_date: date | datetime,
                  end_date: date | datetime,
                  query: str,
                  prefix: str
                  ) -> None:
        date_format = "%Y-%m-%d %H:%M:%S" if type(start_date) == datetime else "%Y-%m-%d"
        start_time = start_date.strftime(date_format)
        end_time = end_date.strftime(date_format)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "STRING", start_time),
                bigquery.ScalarQueryParameter("end_date", "STRING", end_time),
            ]
        )
        df = self._BIGQUERY_CLIENT.query(query=query, job_config=job_config).to_dataframe()
        df.to_parquet(_get_file_name(prefix, start_date))

    def execute(self):
        for config in self._configs:
            start = copy(config.start_date)
            end = copy(config.end_date)
            prefix = config.prefix
            while (end - start).days > 0:
                next_day = start + timedelta(days=1)
                until = date(next_day.year, next_day.month, next_day.day)
                if not os.path.exists(_get_file_name(prefix, start)):
                    print(f"fetching {prefix} data for: {start}")
                    self._get_data(start, until, config.query, prefix)
                start = until
            if config.concat:
                concat(config.prefix, config.start_date, config.end_date)
