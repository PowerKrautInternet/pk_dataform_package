/*config*/
const {isSource, ifNull, ifSource, ref, getRefs, join} = require("../../sources");
let query = `

SELECT 
  'Google Ads' as bron,
  ${ifNull(["ad_group_stats.account", ifSource("stg_google_ads_adgroup_conversions", " ad_group_conversions.account")], " AS account,")}
  ${ifNull(["ad_group_stats.customer_id", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.customer_id")], " AS customer_id,")}
  ${ifNull(["ad_group_stats.campaign_id", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_id")], " AS campaign_id,")}
  ${ifNull(["ad_group_stats.campaign_name", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_name")], " AS campaign_name,")}
  ${ifNull(["ad_group_stats.campaign_advertising_channel_type", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_advertising_channel_type")], " AS campaign_advertising_channel_type,")}
  ${ifNull(["ad_group_stats.campaign_bidding_strategy_type", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_bidding_strategy_type")], " AS campaign_bidding_strategy_type,")}
  ${ifNull(["ad_group_stats.campaign_start_date", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_start_date")], " AS campaign_start_date,")}
  ${ifNull(["ad_group_stats.campaign_end_date", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_end_date")], " AS campaign_end_date,")}
  ${ifNull(["ad_group_stats.campaign_status", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.campaign_status")], " AS campaign_status,")}
  ${ifNull(["ad_group_stats.ad_group_id", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.ad_group_id")], " AS ad_group_id,")}
  ${ifNull(["ad_group_stats.ad_group_name", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.ad_group_name")], " AS ad_group_name,")}
  ${ifNull(["ad_group_stats.ad_group_status", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.ad_group_status")], " AS ad_group_status,")}
  ${ifNull(["ad_group_stats.ad_group_type", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.ad_group_type")], " AS ad_group_type,")}
  ${ifNull(["ad_group_stats.ad_group_bidding_strategy_type", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.ad_group_bidding_strategy_type")], " AS ad_group_bidding_strategy_type,")}
  ${ifNull(["ad_group_stats.segments_date", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.segments_date")], " AS segments_date,")}
  ${ifNull(["ad_group_stats.segments_device", ifSource("stg_google_ads_adgroup_conversions", "  ad_group_conversions.segments_device")], " AS segments_device,")}
  ad_group_stats.impressions,
  ad_group_stats.interactions,
  ad_group_stats.clicks,
  ad_group_stats.Cost,
  ${isSource("stg_google_ads_adgroup_conversions") ? "ad_group_conversions.segments_conversion_action_name" : "STRING(NULL) as segments_conversion_action_name"},
  ${isSource("stg_google_ads_adgroup_conversions") ? "ad_group_conversions.conversions" : "ad_group_stats.conversions"},
  ${isSource("stg_google_ads_adgroup_conversions") ? "ad_group_conversions.conversions_value" : "ad_group_stats.conversions_value"},

FROM(
  SELECT 
    ad_group.account,
    ad_group.customer_id,
    ad_group.campaign_id,
    MAX(ad_campaign.campaign_name) AS campaign_name,
    MAX(ad_campaign.campaign_advertising_channel_type) AS campaign_advertising_channel_type,
    MAX(ad_campaign.campaign_bidding_strategy_type) AS campaign_bidding_strategy_type,
    MAX(ad_campaign.campaign_start_date) AS campaign_start_date,
    MAX(ad_campaign.campaign_end_date) AS campaign_end_date,
    MAX(ad_campaign.campaign_status) AS campaign_status,
    ad_group.ad_group_id,
    ad_group_stats.segments_date AS segments_date,
    ad_group_stats.segments_device,
    MAX(ad_group.ad_group_name) AS ad_group_name,
    MAX(ad_group_status) AS ad_group_status,
    MAX(ad_group_type) AS ad_group_type,
    MAX(ad_group.campaign_bidding_strategy_type) AS ad_group_bidding_strategy_type,
    SUM(ad_group_stats.metrics_impressions) AS impressions,
    SUM(ad_group_stats.metrics_interactions) AS interactions,
    SUM(ad_group_stats.metrics_clicks) AS clicks,
    (SUM(ad_group_stats.metrics_cost_micros) / 1000000) AS Cost,

FROM ${ref('ads_AdGroup')} ad_group 

LEFT JOIN ${ref('ads_AdGroupBasicStats')} ad_group_stats
ON
  ad_group.customer_id = ad_group_stats.customer_id 
  AND ad_group.campaign_id = ad_group_stats.campaign_id
  AND ad_group.ad_group_id = ad_group_stats.ad_group_id

LEFT JOIN (SELECT * FROM ${ref('ads_Campaign')} WHERE _DATA_DATE = _LATEST_DATE) ad_campaign 
ON 
    ad_group.customer_id = ad_campaign.customer_id 
    AND ad_group.campaign_id = ad_campaign.campaign_id

WHERE
ad_group._DATA_DATE = ad_group._LATEST_DATE
AND ad_group_stats.segments_date IS NOT NULL

GROUP BY
    account,
    customer_id,
    campaign_id,
    ad_group_id,
    segments_date,
    segments_device
) ad_group_stats

${join("FULL OUTER JOIN", 'df_staging_views', 'stg_google_ads_adgroup_conversions', 'ad_group_conversions ON 1=0')}
    
    `
let refs = getRefs()
module.exports = {query, refs}
