
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT  
account,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') AS type,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.AccountId') AS account_id,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.AccountName') AS account_name,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.CampaignId') AS campaign_id,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.CampaignName') AS campaign_name,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.TimePeriod') AS time_period,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.AssetGroupId') AS asset_group_id,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AccountStatus') AS account_status,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AssetGroupName') AS asset_group_name,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AssetGroupStatus') AS asset_group_status,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AverageCpc') AS average_cpc,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.CampaignStatus') AS campaign_status,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Clicks') AS clicks,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Conversions') AS conversions,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Ctr') AS ctr,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Impressions') AS impressions,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.ReturnOnAdSpend') AS return_on_ad_spend,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Revenue') AS revenue,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Spend') AS spend

FROM ${ref("bingAdsDataProducer_lasttransaction")}
WHERE JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') = 'AssetGroupPerformanceReportPublisher'
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
