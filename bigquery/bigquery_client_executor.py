import glob
import os
from copy import copy
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from google.oauth2 import service_account
from loguru import logger

from bigquery.bigquery_config import PROJECT_NAME

logger.opt(lazy=True)


@dataclass
class ConfigurationItem:
    start_date: date | datetime | None
    end_date: date | datetime | None
    query: str
    prefix: str
    concat: bool = True


def _get_file_name(prefix: str, start_date: str | None) -> str:
    return (
        f"data/{prefix}_{start_date}.parquet"
        if start_date
        else f"data/{prefix}.parquet"
    )


def concat(prefix: str, start: date, end: date):
    files = glob.glob(f"data/{prefix}*.parquet")
    data_frames = []
    for file in files:
        data_frames.append(pd.read_parquet(file))
    df = pd.concat(data_frames)
    file_name = f"data/{prefix}_{start:%Y-%m-%d}-{end:%Y-%m-%d}.parquet"
    df.to_parquet(file_name)
    logger.info("created file {}", file_name)


class BigQueryClientExecutor(object):
    def __init__(self, configs: Optional[List[ConfigurationItem]] = None):
        load_dotenv()
        self._BIGQUERY_CLIENT_EMAIL = os.getenv("GOOGLE_CLOUD_BIGQUERY_CLIENT_EMAIL")
        self._BIGQUERY_CLIENT_SECRET = os.getenv("GOOGLE_CLOUD_BIGQUERY_CLIENT_SECRET")
        self._GOOGLE_CREDENTIALS = (
            service_account.Credentials.from_service_account_info(
                {
                    "type": "service_account",
                    "project_id": PROJECT_NAME,
                    "private_key": self._BIGQUERY_CLIENT_SECRET.replace("\\n", "\n"),
                    "client_email": self._BIGQUERY_CLIENT_EMAIL,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://accounts.google.com/o/oauth2/token",
                },
            )
        )
        self._BIGQUERY_CLIENT = bigquery.Client(
            credentials=self._GOOGLE_CREDENTIALS, project=PROJECT_NAME
        )
        self._configs = configs

    def _get_data(
        self,
        start_date: date | datetime | None,
        end_date: date | datetime | None,
        query: str,
        prefix: str,
    ) -> None:
        query_parameters = []
        if start_date:
            date_format = (
                "%Y-%m-%d %H:%M:{}" if type(start_date) == datetime else "%Y-%m-%d"
            )
            start_time = start_date.strftime(date_format)
            query_parameters.append(
                bigquery.ScalarQueryParameter("start_date", "STRING", start_time)
            )
        if end_date:
            date_format = (
                "%Y-%m-%d %H:%M:{}" if type(end_date) == datetime else "%Y-%m-%d"
            )
            end_time = end_date.strftime(date_format)
            query_parameters.append(
                bigquery.ScalarQueryParameter("end_date", "STRING", end_time)
            )
        if query_parameters:
            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
            query = self._BIGQUERY_CLIENT.query(query=query, job_config=job_config)
        else:
            query = self._BIGQUERY_CLIENT.query(query=query)
        df = query.to_dataframe()
        file_name = _get_file_name(prefix, start_date)
        df.to_parquet(file_name)
        logger.info("created file {}", file_name)

    def export_data(self):
        for config in self._configs:
            if config.start_date:
                start = copy(config.start_date)
                end = copy(config.end_date)
                prefix = config.prefix
                while (end - start).days > 0:
                    next_day = start + timedelta(days=1)
                    until = date(next_day.year, next_day.month, next_day.day)
                    start_str = start.strftime("%Y-%m-%d")
                    if not os.path.exists(_get_file_name(prefix, start_str)):
                        logger.info("fetching {} data for {}", prefix, start_str)
                        self._get_data(start, until, config.query, prefix)
                    start = until
                if config.concat:
                    concat(prefix, config.start_date, config.end_date)
            else:
                self._get_data(
                    config.start_date, config.end_date, config.query, config.prefix
                )

    def import_data(
        self,
        project_id: str,
        dataset_id: str,
        table_name: str,
        file_name: str,
        date_columns: List[str] = [],
    ):
        # def _lookup(s: dtype) -> str:
        #     match s.name:
        #         case "float64" | "int64" | "bool":
        #             return s.name.upper()
        #         case "datetime64[ns]":
        #             return "DATE"
        #         case _:
        #             return "STRING"

        table_id = ".".join([project_id, dataset_id, table_name])
        suffix = file_name.split(".")[-1]
        dtypes = (
            dict((c, "datetime64[ns]") for c in date_columns)
            if date_columns
            else dict()
        )
        match suffix:
            case "csv":
                df: pd.DataFrame = pd.read_csv(file_name).astype(dtypes)
                source_format = SourceFormat.CSV
            case "parquet":
                df: pd.DataFrame = pd.read_parquet(file_name).astype(dtypes)
                source_format = SourceFormat.PARQUET
            case "json":
                df: pd.DataFrame = pd.read_json(file_name).astype(dtypes)
                source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        logger.info(
            "about to load file {} to {}.{}.{}",
            file_name,
            project_id,
            dataset_id,
            table_name,
        )
        # schema = [
        #     bigquery.SchemaField(name=k, field_type=_lookup(v))
        #     for k, v in df.dtypes.to_dict().items()
        # ]
        job_config = bigquery.LoadJobConfig(
            # schema=schema,
            source_format=source_format,
            write_disposition="WRITE_TRUNCATE",
        )
        # Make an API request.
        job = self._BIGQUERY_CLIENT.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        # Wait for the job to complete.
        job.result()
        table_ref = self._BIGQUERY_CLIENT.get_table(table_id)  # Make an API request.
        logger.info(
            "Loaded %d rows and %d columns to {}",
            table_ref.num_rows,
            len(table_ref.schema),
            table_id,
        )
