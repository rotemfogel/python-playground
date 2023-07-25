from datetime import datetime

from bigquery.bigquery_client_executor import ConfigurationItem, BigQueryClientExecutor
from bigquery.bigquery_config import (
    PROJECT_NAME,
)

__QUERY = f"""
SELECT PARSE_JSON(holiday_types) holiday_types
  FROM {PROJECT_NAME}.recommendation_service_prod.combined_offers
 WHERE holiday_types IS NOT NULL 
 LIMIT 3
"""

if __name__ == "__main__":
    cutoff_date = datetime(2022, 10, 26, 22, 0, 0)
    configuration: list[ConfigurationItem] = [
        ConfigurationItem(
            start_date=None,
            end_date=None,
            query=__QUERY,
            prefix="wtf",
        )
    ]
    BigQueryClientExecutor(configuration).export_data()
