from airfart.google_ads.hooks.google_ads_api_hook import GoogleAdsApiType
from airfart.google_ads.operators.google_ads_api_operator import GoogleAdsApiOperator

if __name__ == "__main__":
    query = '''
    SELECT campaign.id,
       campaign.labels,
       campaign.end_date,
       campaign.start_date,
       campaign.status,
       campaign.name,
       campaign.advertising_channel_type,
       campaign.bidding_strategy_type,
       metrics.impressions,
       metrics.search_impression_share,
       metrics.absolute_top_impression_percentage,
       metrics.top_impression_percentage,
       metrics.clicks,
       metrics.conversions,
       metrics.all_conversions,
       metrics.video_views,
       metrics.bounce_rate,
       metrics.percent_new_visitors,
       metrics.cross_device_conversions,
       metrics.view_through_conversions,
       segments.ad_network_type,
       campaign_budget.amount_micros
  FROM campaign
 WHERE metrics.impressions > 1
   AND campaign.status IN ('ENABLED', 'PAUSED')'''
    operator = GoogleAdsApiOperator(sql=query,
                                    bucket='imaginary-athena-test-bucket',
                                    database='dbl',
                                    table='campaign',
                                    method=GoogleAdsApiType.SearchStream
                                    )
    operator.execute()
