import json
import os

import pendulum
from dotenv import load_dotenv
from pendulum import DateTime

from airfart.google_ads.hooks.google_ads_api_hook import GoogleAdsApiType
from airfart.google_ads.operators.google_ads_api_operator import GoogleAdsApiOperator

if __name__ == "__main__":
    load_dotenv()

    queries = dict(
        campaigns='''SELECT campaign.id,
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
                        AND segments.date = '{date}'
                        AND campaign.status IN ('ENABLED', 'PAUSED')''',
        ad_groups='''SELECT ad_group.id,
                            ad_group.name,
                            ad_group.status,
                            ad_group.campaign,
                            ad_group.labels,
                            ad_group.base_ad_group,
                            campaign.id,
                            campaign.base_campaign,
                            campaign.status,
                            ad_group.cpc_bid_micros,
                            ad_group.target_cpa_micros,
                            segments.date,
                            metrics.clicks,
                            metrics.cost_micros,
                            metrics.impressions,
                            metrics.search_impression_share,
                            metrics.absolute_top_impression_percentage,
                            metrics.top_impression_percentage,
                            metrics.conversions,
                            metrics.all_conversions,
                            metrics.video_views,
                            metrics.bounce_rate,
                            metrics.percent_new_visitors,
                            metrics.cross_device_conversions
                       FROM ad_group
                      WHERE metrics.impressions > 1
                        AND segments.date = '{date}' '''
    )

    accounts = json.loads(os.getenv('google_accounts'))

    start: DateTime = pendulum.DateTime(2022, 4, 25)
    until: DateTime = pendulum.DateTime(2022, 4, 26)

    # dates = [pendulum.DateTime(2022, 4, 17)]
    dates = []
    diff = start.diff(until).in_days()
    for i in range(0, diff):
        dates.append(start.add(days=i))

    table = 'ad_groups'
    google_ads_db = 'googleads'

    for dt in dates:
        for account_id in accounts:
            date_str = dt.format('YYYY-MM-DD')
            try:
                print(f'getting ]{table}[ data for account [{account_id}] at [{date_str}]')
                operator = GoogleAdsApiOperator(
                    sql=queries[table].format(date=date_str),
                    bucket='seekingalpha-data',
                    database='dba',
                    table=table,
                    post_db_path=f'{google_ads_db}/{table}/date_={date_str}/account_id={account_id}',
                    method=GoogleAdsApiType.SearchStream,
                    account_id=account_id
                )
                operator.execute()
            except Exception as e:
                print(f'error fetching {date_str} data for customer_id {account_id}')
                raise e
