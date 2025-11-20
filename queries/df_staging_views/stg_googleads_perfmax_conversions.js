/*config*/

let pk = require("../../sources")
const {ifSource, join} = require("../../sources");
let ref = pk.ref

let query = `

-- his Query is dependant on ads_CampaignConversionStats

SELECT 
    ad_campaign.account,
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
    MAX(campaign_conversions.segments_conversion_action_category) AS segments_conversion_action_category,
    SUM(campaign_conversions.metrics_conversions) AS conversions,
    SUM(campaign_conversions.metrics_conversions_value) AS conversions_value,

FROM ${ref('ads_Campaign')} ad_campaign 

LEFT JOIN${ref("ads_CampaignConversionStats")} campaign_conversions
    ON ad_campaign.customer_id = campaign_conversions.customer_id 
    AND ad_campaign.campaign_id = campaign_conversions.campaign_id

WHERE
ad_campaign._DATA_DATE = ad_campaign._LATEST_DATE
AND ad_campaign.campaign_advertising_channel_type = 'PERFORMANCE_MAX'
AND campaign_conversions.segments_date IS NOT NULL

GROUP BY
    account,
    customer_id,
    campaign_id, 
    segments_date,
    segments_conversion_action_name
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}
