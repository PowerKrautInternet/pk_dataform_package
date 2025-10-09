/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT 
    ad_campaign.customer_id,
    ad_campaign.campaign_id,
    MAX(ad_campaign.campaign_name) AS campaign_name,
    MAX(ad_campaign.campaign_advertising_channel_type) AS campaign_advertising_channel_type,
    MAX(ad_campaign.campaign_bidding_strategy_type) AS campaign_bidding_strategy_type,
    MAX(ad_campaign.campaign_start_date) AS campaign_start_date,
    MAX(ad_campaign.campaign_end_date) AS campaign_end_date,
    MAX(ad_campaign.campaign_status) AS campaign_status,
    campaign_conversions.segments_date AS segments_date,
    campaign_conversions.segments_conversion_action_name,
    SUM(campaign_conversions.metrics_conversions) AS conversions,
    SUM(campaign_conversions.metrics_conversions_value) AS conversions_value

FROM ${ref('ads_Campaign_7594935172')} ad_campaign 

LEFT JOIN ${ref('ads_CampaignConversionStats_7594935172')} campaign_conversions
ON ad_campaign.customer_id = campaign_conversions.customer_id 
AND ad_campaign.campaign_id = campaign_conversions.campaign_id

WHERE
ad_campaign._DATA_DATE = ad_campaign._LATEST_DATE
AND ad_campaign.campaign_advertising_channel_type = 'PERFORMANCE_MAX'
AND campaign_conversions.segments_date IS NOT NULL

GROUP BY
    customer_id,
    campaign_id,
    segments_date,
    segments_conversion_action_name
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}
