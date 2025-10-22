/*config*/
const {join, ref, getRefs, ifSource, ifNull, isSource} = require("../../sources");
let query = `

SELECT 
  'Google Ads' as bron,
  ${ifNull(["campaign_stats.account", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.account")], "AS account,")}
  ${ifNull(["campaign_stats.customer_id", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.customer_id")], "AS customer_id,")}
  ${ifNull(["campaign_stats.campaign_id", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_id")], "AS campaign_id,")}
  ${ifNull(["campaign_stats.campaign_name", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_name")], "AS campaign_name,")}
  ${ifNull(["campaign_stats.campaign_advertising_channel_type", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_advertising_channel_type")], "AS campaign_advertising_channel_type,")}
  ${ifNull(["campaign_stats.campaign_bidding_strategy_type", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_bidding_strategy_type")], "AS campaign_bidding_strategy_type,")}
  ${ifNull(["campaign_stats.campaign_start_date", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_start_date")], "AS campaign_start_date,")}
  ${ifNull(["campaign_stats.campaign_end_date", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_end_date")], "AS campaign_end_date,")}
  ${ifNull(["campaign_stats.campaign_status", ifSource("stg_googleads_perfmax_conversions", "campaign_conversions.campaign_status")], "AS campaign_status,")}
  ${ifNull(["campaign_stats.segments_date", ifSource("ads_CampaignConversionStats", "campaign_conversions.segments_date")], "AS segments_date,")}
  campaign_stats.impressions,
  campaign_stats.interactions,
  campaign_stats.clicks,
  campaign_stats.Cost,
  ${isSource("ads_CampaignConversionStats") ? "campaign_conversions.segments_conversion_action_name" : "STRING(NULL) as segments_conversion_action_name"},
  ${isSource("ads_CampaignConversionStats") ? "campaign_conversions.conversions" : "campaign_stats.conversions"},
  ${isSource("ads_CampaignConversionStats") ? "campaign_conversions.conversions_value" : "campaign_stats.conversions_value"},

FROM(
  SELECT 
    ad_campaign.account,
    ad_campaign.customer_id,
    ad_campaign.campaign_id,
    campaign_stats.segments_date AS segments_date,
    MAX(ad_campaign.campaign_name) AS campaign_name,
    MAX(ad_campaign.campaign_advertising_channel_type) AS campaign_advertising_channel_type,
    MAX(ad_campaign.campaign_bidding_strategy_type) AS campaign_bidding_strategy_type,
    MAX(ad_campaign.campaign_start_date) AS campaign_start_date,
    MAX(ad_campaign.campaign_end_date) AS campaign_end_date,
    MAX(ad_campaign.campaign_status) AS campaign_status,
    SUM(campaign_stats.metrics_impressions) AS impressions,
    SUM(campaign_stats.metrics_interactions) AS interactions,
    SUM(campaign_stats.metrics_clicks) AS clicks,
    (SUM(campaign_stats.metrics_cost_micros) / 1000000) AS Cost,
    SUM(campaign_stats.metrics_conversions) AS conversions,
    SUM(campaign_stats.metrics_conversions_value) AS conversions_value,

FROM ${ref('ads_Campaign')} ad_campaign 

LEFT JOIN ${ref('ads_CampaignBasicStats')} campaign_stats
ON
  ad_campaign.customer_id = campaign_stats.customer_id 
  AND ad_campaign.campaign_id = campaign_stats.campaign_id

WHERE
ad_campaign._DATA_DATE = ad_campaign._LATEST_DATE
AND ad_campaign.campaign_advertising_channel_type = 'PERFORMANCE_MAX'
AND campaign_stats.segments_date IS NOT NULL

GROUP BY
    account,
    customer_id,
    campaign_id,
    segments_date
) campaign_stats

${ifSource("stg_googleads_perfmax_conversions", `
    FULL OUTER JOIN ${ref('df_staging_views', 'stg_googleads_perfmax_conversions')} campaign_conversions
    ON 1=0
`)}

`
let refs = getRefs()
module.exports = {query, refs}
