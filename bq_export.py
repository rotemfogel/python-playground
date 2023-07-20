from datetime import date

from bigquery.bigquery_client_executor import BigQueryClientExecutor, ConfigurationItem
from bigquery.bigquery_config import PROD_ANALYTICS_PROJECT_NAME

__QUERY = f"""
SELECT *
  FROM `{PROD_ANALYTICS_PROJECT_NAME}`.lere_stage.stage_context_views
 WHERE view_date_utc >= DATE(@start_date)
   AND view_date_utc <  DATE(@end_date)
"""

if __name__ == "__main__":
    config = [
        ConfigurationItem(
            start_date=date(2019, 9, 9),
            end_date=date.today(),
            query=__QUERY,
            prefix="context_views",
            concat=True,
        )
    ]
    BigQueryClientExecutor(config).export_data()
