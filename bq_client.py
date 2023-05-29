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
VIEW_COLUMN = os.getenv("VIEW_COLUMN")
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
WITH sessions AS (
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
    JOIN UNNEST({VIEW_COLUMN}) pv
   WHERE derived_tstamp >= TIMESTAMP(@start_date) 
     AND derived_tstamp <  TIMESTAMP(@end_date)
     AND {VIEW_COLUMN} IS NOT NULL
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
  LEFT JOIN `{PROJECT_NAME}`.recommendation_service_prod.combined_offers co
    ON (av.offer_bk = co.offer_id)
"""

HEAP_ABANDONED_CARTS = f"""
WITH sessions AS (
   SELECT time, 
          user_id, 
          session_id, 
          CASE ARRAY_LENGTH(array_reverse(split(path, '/')))
            WHEN 6 THEN array_reverse(split(path, '/'))[offset(1)]
            ELSE array_reverse(split(path, '/'))[offset(0)]
          END AS offer_bk,
          CASE
            WHEN CONTAINS_SUBSTR(path, 'checkout')
              THEN 'checkout'
            WHEN CONTAINS_SUBSTR(path, 'offer') OR CONTAINS_SUBSTR(path, 'partner')
              THEN 'page_view'
            WHEN CONTAINS_SUBSTR(path, 'booking')
              THEN 'booking'
          END event_type,
     FROM `{PROJECT_NAME}`.heap_migrated.all_events
    WHERE event_view_name = 'pageviews'
      AND time >= TIMESTAMP(@start_date) 
      AND time <  TIMESTAMP(@end_date)
      AND (   CONTAINS_SUBSTR(path, 'checkout') 
           OR CONTAINS_SUBSTR(path, 'offer')
           OR CONTAINS_SUBSTR(path, 'partner')
           OR CONTAINS_SUBSTR(path, 'booking')) 
      AND session_id IS NOT NULL
      AND user_id IS NOT NULL
),
candidate_events AS (
  SELECT time,
         session_id, 
         user_id,
         offer_bk,
         event_type,
         LEAD(event_type) OVER (PARTITION BY user_id, session_id ORDER BY time) AS next_event_type
    FROM sessions
),
abandoned_views AS (
  SELECT MAX(DATETIME(e.time, 'Australia/Sydney')) AS view_datetime_sydt,
         e.session_id                              AS domain_sessionid,
         customer_bk,
         offer_bk,
         c.customer_salesforce_contact_bk          AS subscriber_key
    FROM candidate_events ce
    JOIN `{PROJECT_NAME}`.heap_migrated.all_events e
      ON (    ce.user_id = e.user_id
          AND ce.time = e.time
          AND ce.session_id = e.session_id)
    JOIN `{PROJECT_NAME}`.heap_migrated.users u
      ON (u.user_id = e.user_id)
    LEFT JOiN `{PROD_ANALYTICS_PROJECT_NAME}`.datawarehouse.dim_customer c 
      ON (u.identity = c.customer_bk)
   WHERE e.time >= TIMESTAMP(@start_date) 
     AND e.time <  TIMESTAMP(@end_date)
     AND event_type = 'page_view'
     AND next_event_type IN ('checkout','booking')
     AND offer_bk IS NOT NULL
   GROUP BY e.session_id, customer_bk, offer_bk, identity, customer_salesforce_contact_bk
)
SELECT av.*,
       co.country_code, 
       offer_type,
       offer_end_date,
       holiday_types
  FROM abandoned_views av
  LEFT JOIN `{PROJECT_NAME}`.recommendation_service_prod.combined_offers co
    ON (av.offer_bk = co.offer_id)"""


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
    concat(conf.prefix, start, end)


def main():
    cutoff_date = datetime(2022, 10, 26, 22, 0, 0)
    configuration: list[ConfigurationItem] = [
        ConfigurationItem(
            start_date=datetime(2021, 9, 9, 7, 20, 0, 0),
            end_date=cutoff_date,
            query=HEAP_ABANDONED_CARTS,
            prefix="heap_cart_offers",
        ),
        # ConfigurationItem(
        #     start_date=cutoff_date,
        #     end_date=datetime.now(),
        #     query=ABANDONED_CART_VIEWS,
        #     prefix="abandoned_cart_offers",
        # ),
    ]
    for conf in configuration:
        loop(conf)


def concat(prefix: str, start: datetime, end: datetime):
    files = glob.glob(f"data/{prefix}*.parquet")
    data_frames = []
    for file in files:
        data_frames.append(pd.read_parquet(file))
    df = pd.concat(data_frames)
    df.to_parquet(f"data/{prefix}_{start:%Y-%m-%d}-{end:%Y-%m-%d}.parquet")


if __name__ == "__main__":
    main()
