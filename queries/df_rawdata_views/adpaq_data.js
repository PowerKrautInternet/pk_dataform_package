/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_VALUE(PAYLOAD, '$.type') AS type,
  PARSE_DATE('%d/%m/%Y', JSON_VALUE(PAYLOAD, '$.date')) AS date,
  JSON_VALUE(PAYLOAD, '$.campaignName') AS campaign_name,
  JSON_VALUE(PAYLOAD, '$.creativeName') AS creative_name,
  JSON_VALUE(PAYLOAD, '$.publisherName') AS publisher_name,
  JSON_VALUE(PAYLOAD, '$.deviceType') AS device_type,
  JSON_VALUE(PAYLOAD, '$.response.instanceName') AS instance_name,
  JSON_VALUE(PAYLOAD, '$.response.agencyName') AS agency_name,
  JSON_VALUE(PAYLOAD, '$.response.advertiserName') AS advertiser_name,
  JSON_VALUE(PAYLOAD, '$.response.campaignId') AS campaign_id,
  JSON_VALUE(PAYLOAD, '$.response.lineItemName') AS line_item_name,
  JSON_VALUE(PAYLOAD, '$.response.station') AS station,
  JSON_VALUE(PAYLOAD, '$.response.dealId') AS deal_id,
  JSON_VALUE(PAYLOAD, '$.response.rollout') AS rollout,
  JSON_VALUE(PAYLOAD, '$.response.regionName1') AS region_name_1,
  JSON_VALUE(PAYLOAD, '$.response.regionName2') AS region_name_2,
  JSON_VALUE(PAYLOAD, '$.response.postalCode') AS postal_code,
  JSON_VALUE(PAYLOAD, '$.response.sspName') AS ssp_name,
  JSON_VALUE(PAYLOAD, '$.response.currency') AS currency,
  CAST(JSON_VALUE(PAYLOAD, '$.response.impressions') AS INT64) AS impressions,
  CAST(JSON_VALUE(PAYLOAD, '$.response.techFees') AS FLOAT64) AS tech_fees,
  CAST(JSON_VALUE(PAYLOAD, '$.response.agencyFees') AS FLOAT64) AS agency_fees,
  CAST(JSON_VALUE(PAYLOAD, '$.response.budgetSpentWithMarkup') AS FLOAT64) AS budget_spent_with_markup,
  CAST(JSON_VALUE(PAYLOAD, '$.response.ecpm') AS FLOAT64) AS ecpm,
  CAST(JSON_VALUE(PAYLOAD, '$.response.budgetSpent') AS FLOAT64) AS budget_spent

FROM ${ref("df_rawdata_views", "adpaqDataProducer_lasttransaction")}

WHERE JSON_VALUE(PAYLOAD, '$.type') = 'AdPaqReportPublisher'

`
let refs = pk.getRefs()
module.exports = {query, refs}
