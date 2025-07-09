/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT 
    'Google Ads' as bron,
    ad_group.customer_id as account_id,
    MAX(ad_group.account) AS account_name,
    ad_group.campaign_id,
    MAX(ad_campaign.campaign_name) AS campaign_name,
    MAX(ad_campaign.campaign_advertising_channel_type) AS campaign_advertising_channel_type,
    MAX(ad_campaign.campaign_bidding_strategy_type) AS campaign_bidding_strategy_type,
    MAX(ad_campaign.campaign_start_date) AS campaign_start_date,
    MAX(ad_campaign.campaign_end_date) AS campaign_end_date,
    MAX(ad_campaign.campaign_status) AS campaign_status,
    ad_group.ad_group_id,
    ad_group_stats.segments_date AS segments_date,
    --segments_device,
    MAX(ad_group.ad_group_name) AS ad_group_name,
    MAX(ad_group_status) AS ad_group_status,
    MAX(ad_group_type) AS ad_group_type,
    MAX(ad_group.campaign_bidding_strategy_type) AS ad_group_bidding_strategy_type,
    SUM(ad_group_stats.metrics_impressions) AS impressions,
    SUM(ad_group_stats.metrics_interactions) AS interactions,
    SUM(ad_group_stats.metrics_clicks) AS clicks,
    SUM(ad_group_stats.metrics_conversions) AS conversions,
    SUM(ad_group_stats.metrics_conversions_value) AS conversions_value,
    (SUM(ad_group_stats.metrics_cost_micros) / 1000000) AS Cost,
    /*SUM(ad_group_conversions.metrics_conversions) AS metrics_conversions, 
    SUM(ad_group_conversions.metrics_conversions_value) AS metrics_conversions_value, 
    SUM(IF(LOWER(segments_conversion_action_name) LIKE "%werkplaats%", ad_group_conversions.metrics_conversions, 0)) AS conversions_werkplaats,
    SUM(IF(LOWER(segments_conversion_action_name) LIKE "%offerte%", ad_group_conversions.metrics_conversions, 0)) AS conversions_offerte,
    SUM(IF(LOWER(segments_conversion_action_name) LIKE "%tel_klik%", ad_group_conversions.metrics_conversions, 0)) AS conversions_tel_klik,
    SUM(IF(segments_conversion_action_category LIKE "%SUBMIT_LEAD_FORM%", ad_group_conversions.metrics_conversions, 0)) AS conversions_lead_form, */


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
    account_id,
    campaign_id,
    ad_group_id,
    segments_date
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}
