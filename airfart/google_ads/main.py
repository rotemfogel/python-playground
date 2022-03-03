import json
import os

import pyarrow as pa
from dotenv import load_dotenv
from pandas import DataFrame

from airfart.google_ads.hooks.google_ads_api_hook import GoogleAdsApiType
from airfart.google_ads.operators.google_ads_api_operator import GoogleAdsApiOperator

__labels = "labels"


def get_clean_array(array):
    if pa.types.is_struct(array.type):
        if isinstance(array, pa.ChunkedArray):
            array = array.combine_chunks()
        arrays = [
            get_clean_array(array.field(index))
            for index, field in enumerate(array.type)
            if not pa.types.is_null(field.type)
        ]
        names = [
            field.name
            for field in array.type
            if not pa.types.is_null(field.type)
        ]
        return pa.StructArray.from_arrays(arrays, names)
    else:
        return array


def get_clean_table(table):
    return pa.Table.from_pydict({
        field.name: get_clean_array(table[field.name])
        for field in table.schema if not pa.types.is_null(field.type)
    })


def drop_labels(df: DataFrame) -> DataFrame:
    """
    drop the "labels" column from the campaign data
    since data is inconsistent across different campaigns
    """
    # Convert from pandas to Arrow
    table = pa.Table.from_pandas(df)
    clean_table = get_clean_table(table)
    return df


if __name__ == "__main__":
    load_dotenv()
    query = '''
    SELECT campaign.id,
       campaign.name,
       campaign.status,
       campaign.base_campaign,
       campaign.advertising_channel_type,
       campaign.labels,
       campaign.bidding_strategy_type,
       campaign.frequency_caps,
       campaign.experiment_type,
       campaign_budget.amount_micros,
       segments.ad_network_type,
       segments.hour,
       segments.date,
       metrics.cost_micros,
       metrics.impressions,
       metrics.search_impression_share,
       metrics.clicks,
       metrics.conversions,
       metrics.all_conversions,
       metrics.video_views,
       metrics.view_through_conversions
  FROM campaign
 WHERE metrics.impressions > 1
   AND segments.date = '2022-01-17'
   AND campaign.status IN ('ENABLED', 'PAUSED')'''

    accounts = json.loads(os.getenv('google_accounts'))
    for account_id in accounts:
        try:
            operator = GoogleAdsApiOperator(sql=query,
                                            bucket='seekingalpha-data',
                                            database='googleads',
                                            table='campaigns',
                                            method=GoogleAdsApiType.SearchStream,
                                            customer_id=account_id,
                                            records_transform_fn=drop_labels
                                            )
            operator.execute()
        except Exception as e:
            print(f'error fetching customer_id {account_id}')
            raise e
