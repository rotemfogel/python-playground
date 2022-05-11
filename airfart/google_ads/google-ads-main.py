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
                        AND segments.date = '{date}' ''',
        geo='''SELECT geographic_view.country_criterion_id,
                      geographic_view.resource_name,
                      geographic_view.location_type,
                      ad_group.id,
                      ad_group.name,
                      campaign.id,
                      campaign.name,
                      campaign.start_date,
                      segments.device,
                      segments.date,
                      metrics.impressions,
                      metrics.average_cpc,
                      metrics.clicks,
                      metrics.video_views,
                      metrics.interactions,
                      metrics.ctr,
                      metrics.all_conversions,
                      metrics.conversions,
                      metrics.view_through_conversions,
                      metrics.cross_device_conversions
                 FROM geographic_view
                WHERE metrics.impressions > 1
                  AND segments.date = '{date}'
            ''',
        keywords='''SELECT campaign.base_campaign,
                           campaign.bidding_strategy,
                           campaign.id,
                           campaign.labels,
                           campaign.name,
                           campaign.status,
                           ad_group.base_ad_group,
                           ad_group.id,
                           ad_group.labels,
                           ad_group.name,
                           ad_group.target_cpa_micros,
                           ad_group.percent_cpc_bid_micros,
                           ad_group.status,
                           ad_group_criterion.keyword.text, 
                           ad_group_criterion.keyword.match_type,
                           metrics.absolute_top_impression_percentage,
                           metrics.all_conversions,
                           metrics.clicks,
                           metrics.conversions,
                           metrics.engagements,
                           metrics.impressions,
                           metrics.historical_quality_score,
                           metrics.historical_search_predicted_ctr,
                           metrics.historical_landing_page_quality_score,
                           metrics.historical_creative_quality_score,
                           metrics.interactions,
                           metrics.search_impression_share,
                           metrics.search_top_impression_share,
                           metrics.video_views,
                           metrics.average_cpc,
                           metrics.cross_device_conversions,
                           segments.date 
                      FROM keyword_view
                     WHERE metrics.impressions > 1
                       AND segments.date = '{date}'
        ''',
        landing_pages='''SELECT ad_group.base_ad_group, 
                                ad_group.campaign, 
                                ad_group.id, 
                                ad_group.name, 
                                ad_group.labels, 
                                campaign.base_campaign, 
                                campaign.bidding_strategy, 
                                campaign.final_url_suffix, 
                                campaign.id, 
                                campaign.name, 
                                campaign.serving_status, 
                                segments.ad_network_type, 
                                segments.device, 
                                segments.date, 
                                metrics.all_conversions, 
                                metrics.average_cpc, 
                                metrics.clicks, 
                                metrics.conversions, 
                                metrics.cross_device_conversions, 
                                metrics.engagements, 
                                metrics.impressions, 
                                metrics.speed_score, 
                                metrics.valid_accelerated_mobile_pages_clicks_percentage, 
                                metrics.video_views, 
                                customer.id 
                           FROM landing_page_view
                          WHERE metrics.impressions > 1
                            AND segments.date = '{date}'
        '''
    )

    accounts = json.loads(os.getenv('google_accounts'))

    start: DateTime = pendulum.DateTime(2021, 1, 10)
    until: DateTime = pendulum.DateTime(2022, 5, 11)

    # dates = [pendulum.DateTime(2022, 4, 17)]
    dates = []
    diff = start.diff(until).in_days()
    for i in range(0, diff):
        dates.append(start.add(days=i))

    table = 'landing_pages'
    google_ads_db = 'googleads'

    for dt in dates:
        for account_id in accounts:
            date_str = dt.format('YYYY-MM-DD')
            try:
                print(f'getting [{table}] data for account [{account_id}] at [{date_str}]')
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
