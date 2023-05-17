import glob
import os
from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
SOURCE_COLUMN = os.getenv("SOURCE_COLUMN")
PROD_ANALYTICS_PROJECT_NAME = os.getenv("PROD_ANALYTICS_PROJECT_NAME")
SNOWPLOW_PROJECT_NAME = os.getenv("SNOWPLOW_PROJECT_NAME")
PROJECT_NAME = os.getenv("GOOGLE_CLOUD_BIGQUERY_PROJECT_ID")
BIGQUERY_CLIENT_EMAIL = os.getenv("GOOGLE_CLOUD_BIGQUERY_CLIENT_EMAIL")
BIGQUERY_CLIENT_SECRET = os.getenv("GOOGLE_CLOUD_BIGQUERY_CLIENT_SECRET")
GOOGLE_CREDENTIALS = None
GOOGLE_CREDENTIALS = service_account.Credentials.from_service_account_info(
    {
        "type": "service_account",
        "project_id": PROJECT_NAME,
        "private_key": BIGQUERY_CLIENT_SECRET.replace("\\n", "\n"),
        "client_email": BIGQUERY_CLIENT_EMAIL,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
    },
)

ABANDONED_CART_VIEWS = f"""
WITH sessions AS(
   SELECT derived_tstamp,
          domain_sessionid,
          user_id, 
          CASE
            WHEN CONTAINS_SUBSTR(pv.path_template, 'checkout')
              THEN 'checkout'
            WHEN CONTAINS_SUBSTR(pv.path_template, 'offerId')
              THEN 'page_view'
            WHEN CONTAINS_SUBSTR(pv.path_template, 'booking')
              THEN 'booking'
          END event_type,
          JSON_VALUE(pv.path_params, '$.offerId') AS offer_bk
    FROM `{SNOWPLOW_PROJECT_NAME}`.snowplow.events e 
    JOIN UNNEST(contexts_com_luxgroup_page_view_context_1_0_0) pv
   WHERE derived_tstamp >= TIMESTAMP(@start_date) 
     AND derived_tstamp <  TIMESTAMP(@end_date)
     AND contexts_com_luxgroup_page_view_context_1_0_0 IS NOT NULL
     AND (   CONTAINS_SUBSTR(pv.path_template, 'checkout') 
          OR CONTAINS_SUBSTR(pv.path_template, 'offerId')
          OR CONTAINS_SUBSTR(pv.path_template, 'booking'))
     AND domain_sessionid IS NOT NULL
     AND user_id IS NOT NULL
),
candidate_events AS (
  SELECT derived_tstamp,
         domain_sessionid, 
         user_id AS customer_bk,
         offer_bk,
         event_type,
         LEAD(event_type) OVER (PARTITION BY user_id, domain_sessionid ORDER BY derived_tstamp) AS next_event_type
    FROM sessions
),
abandoned_views AS (
  SELECT MAX(DATETIME(e.derived_tstamp, 'Australia/Sydney')) AS view_datetime_sydt,
         e.domain_sessionid,
         e.user_id                                           AS customer_bk,
         offer_bk,
         c.customer_salesforce_contact_bk AS subscriber_key
    FROM candidate_events ce
    JOIN `{SNOWPLOW_PROJECT_NAME}`.snowplow.events e
      ON (   ce.customer_bk = e.user_id
          AND ce.derived_tstamp = e.derived_tstamp
          AND ce.domain_sessionid = e.domain_sessionid)
    JOIN `{PROD_ANALYTICS_PROJECT_NAME}`.datawarehouse.dim_customer c 
   USING (customer_bk)
   WHERE e.derived_tstamp >= TIMESTAMP(@start_date) 
     AND e.derived_tstamp <  TIMESTAMP(@end_date)
     AND event_type = 'page_view'
     AND next_event_type IN ('checkout','booking')
     AND offer_bk IS NOT NULL
   GROUP BY domain_sessionid, user_id, offer_bk, c.customer_salesforce_contact_bk
)
SELECT av.*,
       co.country_code, 
       offer_type,
       offer_end_date,
       holiday_types
  FROM abandoned_views av
  LEFT JOIN {PROJECT_NAME}.recommendation_service_prod.combined_offers co
    ON (av.offer_bk = co.offer_id)
"""

ABANDONED_CART_SVC = f"""
SELECT DATETIME(derived_tstamp, 'Australia/Sydney')        AS view_datetime_sydt,
       NULL as domain_sessionid,
       COALESCE({SOURCE_COLUMN}_1_1_2.id_member,
                {SOURCE_COLUMN}_1_1_1.id_member)           AS customer_bk,
       COALESCE(c_1_1_2.offer_id, c_1_1_1.offer_id)        AS offer_bk,
       co.country_code, 
       COALESCE(co.offer_type,  
         COALESCE(c_1_1_2.offer_type, c_1_1_1.offer_type)) AS offer_type, 
       offer_end_date,
       holiday_types
  FROM `{SNOWPLOW_PROJECT_NAME}`.snowplow.events e
  JOIN UNNEST ({SOURCE_COLUMN}_1_1_2.cart_items) AS c_1_1_2
  JOIN UNNEST ({SOURCE_COLUMN}_1_1_1.cart_items) AS c_1_1_1 
  LEFT JOIN `{PROJECT_NAME}`.recommendation_service_prod.combined_offers co
    ON (COALESCE(c_1_1_2.offer_id, c_1_1_1.offer_id) = co.offer_id)
 WHERE derived_tstamp >= TIMESTAMP(@start_date)
   AND derived_tstamp <  TIMESTAMP(@end_date)
   AND app_id = 'svc-cart'
   AND (   {SOURCE_COLUMN}_1_1_1 IS NOT NULL
        OR  {SOURCE_COLUMN}_1_1_2 IS NOT NULL)
"""


@dataclass
class ConfigurationItem:
    start_date: datetime
    end_date: datetime
    query: str
    prefix: str


def _get_bq_client():
    """Create a big query client for root project"""
    return bigquery.Client(credentials=GOOGLE_CREDENTIALS, project=PROJECT_NAME)


def _get_file_name(prefix: str, start_date: datetime) -> str:
    return f"data/{prefix}_{start_date.strftime('%Y-%m-%d')}.parquet"


def _get_data(
    start_date: datetime, end_date: datetime, query: str, prefix: str
) -> None:
    start_time = start_date.strftime("%Y-%m-%d %H:%M:%S")
    end_time = end_date.strftime("%Y-%m-%d %H:%M:%S")
    client = _get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_time),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_time),
        ]
    )
    df = client.query(query=query, job_config=job_config).to_dataframe()
    df.to_parquet(_get_file_name(prefix, start_date))


def loop(conf: ConfigurationItem) -> None:
    start = copy(conf.start_date)
    end = copy(conf.end_date)
    prefix = conf.prefix
    while (end - start).days > 0:
        next_day = (start + timedelta(days=1)).date()
        until = datetime(next_day.year, next_day.month, next_day.day)
        if not os.path.exists(_get_file_name(prefix, start)):
            print(f"fetching {prefix} data from: {start} to {until}")
            _get_data(start, until, conf.query, prefix)
        start = until


def main():
    run_until = datetime.now()
    configuration: list[ConfigurationItem] = [
        ConfigurationItem(
            start_date=datetime(2023, 2, 3, 14, 0, 0),
            end_date=run_until,
            query=ABANDONED_CART_VIEWS,
            prefix="abandoned_cart_offers",
        ),
        ConfigurationItem(
            start_date=datetime(2023, 2, 3, 14, 0, 0),
            end_date=run_until,
            query=ABANDONED_CART_SVC,
            prefix="abandoned_carts",
        ),
    ]
    for conf in configuration:
        loop(conf)


if __name__ == "__main__":
    files = glob.glob("data/*.parquet")
    data_frames = []
    for file in files:
        data_frames.append(pd.read_parquet(file))
    df = pd.concat(data_frames)
    df.to_parquet("data/abandoned_cart_offers_2022-10-26-2023-03-17.parquet")
