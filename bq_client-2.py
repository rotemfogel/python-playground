from datetime import datetime

from bigquery.bigquery_client_executor import ConfigurationItem, BigQueryClientExecutor
from bigquery.bigquery_config import SNOWPLOW_PROJECT_NAME, VIEW_COLUMN, PROD_ANALYTICS_PROJECT_NAME, PROJECT_NAME

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
         CASE ARRAY_LENGTH(SPLIT(path, '/'))
           WHEN 6 THEN ARRAY_REVERSE(SPLIT(path, '/'))[offset(1)]
           ELSE ARRAY_REVERSE(SPLIT(path, '/'))[offset(0)]
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
    LEFT JOIN `{PROD_ANALYTICS_PROJECT_NAME}`.datawarehouse.dim_customer c 
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

if __name__ == "__main__":
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
    BigQueryClientExecutor(configuration).execute()
