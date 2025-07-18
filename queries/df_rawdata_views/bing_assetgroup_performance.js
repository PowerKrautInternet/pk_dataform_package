
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT  
  account,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') AS type,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.AccountId') AS AccountId,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.AccountName') AS AccountName,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.CampaignId') AS CampaignId,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.CampaignName') AS CampaignName,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.TimePeriod') AS TimePeriod,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.AssetGroupId') AS AssetGroupId,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AccountStatus') AS AccountStatus,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AssetGroupName') AS AssetGroupName,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AssetGroupStatus') AS AssetGroupStatus,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.AverageCpc') AS AverageCpc,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.CampaignStatus') AS CampaignStatus,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Clicks') AS Clicks,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Conversions') AS Conversions,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Ctr') AS Ctr,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Impressions') AS Impressions,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.ReturnOnAdSpend') AS ReturnOnAdSpend,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Revenue') AS Revenue,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.Spend') AS Spend

FROM ${ref("bingAdsDataProducer_lasttransaction")}
WHERE JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') = 'AssetGroupPerformanceReportPublisher'
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
