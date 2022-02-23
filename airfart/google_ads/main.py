import json
import os

from dotenv import load_dotenv

from airfart.google_ads.hooks.google_ads_api_hook import GoogleAdsApiType
from airfart.google_ads.operators.google_ads_api_operator import GoogleAdsApiOperator

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
                                            database='google-ads',
                                            table='campaigns',
                                            method=GoogleAdsApiType.SearchStream,
                                            customer_id=account_id
                                            )
            operator.execute()
        except Exception as e:
            print(f'error fetching customer_id {account_id}')
            raise e
