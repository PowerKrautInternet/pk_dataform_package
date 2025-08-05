
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT  
  account,
  JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_VALUE(PAYLOAD, '$.type') AS type,
  JSON_VALUE(PAYLOAD, '$.AccountId') AS account_id,
  JSON_VALUE(PAYLOAD, '$.AccountName') AS account_name,
  JSON_VALUE(PAYLOAD, '$.CampaignId') AS campaign_id,
  JSON_VALUE(PAYLOAD, '$.CampaignName') AS campaign_name,
  JSON_VALUE(PAYLOAD, '$.TimePeriod') AS time_period,
  JSON_VALUE(PAYLOAD, '$.AssetGroupId') AS asset_group_id,
  JSON_VALUE(PAYLOAD, '$.response.AccountStatus') AS account_status,
  JSON_VALUE(PAYLOAD, '$.response.AssetGroupName') AS asset_group_name,
  JSON_VALUE(PAYLOAD, '$.response.AssetGroupStatus') AS asset_group_status,
  JSON_VALUE(PAYLOAD, '$.response.AverageCpc') AS average_cpc,
  JSON_VALUE(PAYLOAD, '$.response.CampaignStatus') AS campaign_status,
  JSON_VALUE(PAYLOAD, '$.response.Clicks') AS clicks,
  JSON_VALUE(PAYLOAD, '$.response.Conversions') AS conversions,
  JSON_VALUE(PAYLOAD, '$.response.Ctr') AS ctr,
  JSON_VALUE(PAYLOAD, '$.response.Impressions') AS impressions,
  JSON_VALUE(PAYLOAD, '$.response.ReturnOnAdSpend') AS return_on_ad_spend,
  JSON_VALUE(PAYLOAD, '$.response.Revenue') AS revenue,
  JSON_VALUE(PAYLOAD, '$.response.Spend') AS spend

FROM ${ref("bingAdsDataProducer_lasttransaction")}
WHERE JSON_VALUE(PAYLOAD, '$.type') = 'AssetGroupPerformanceReportPublisher'
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
