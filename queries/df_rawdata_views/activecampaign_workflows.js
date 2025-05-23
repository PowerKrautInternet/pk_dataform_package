/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT 
  JSON_VALUE(PAYLOAD, "$.dtcmedia_crm_id") AS dtcmedia_crm_id,
  JSON_VALUE(PAYLOAD, "$.type") AS type,
  JSON_VALUE(PAYLOAD, "$.id") AS campaign_id,
  PARSE_DATE('%d-%m-%Y', JSON_VALUE(PAYLOAD, "$.date")) AS campaign_date,
  JSON_VALUE(PAYLOAD, "$.response.name") AS flow_name,
  JSON_VALUE(PAYLOAD, "$.response.status") AS status,
  CAST(JSON_VALUE(PAYLOAD, "$.response.contacts_entered") AS INT64) AS contacts_entered,
  CAST(JSON_VALUE(PAYLOAD, "$.response.campaigns") AS INT64) AS campaigns,
  CAST(JSON_VALUE(PAYLOAD, "$.response.total_sends") AS INT64) AS total_sends,
  CAST(JSON_VALUE(PAYLOAD, "$.response.unique_opens") AS INT64) AS unique_opens,
  CAST(JSON_VALUE(PAYLOAD, "$.response.total_clicks") AS INT64) AS total_clicks,
  CAST(JSON_VALUE(PAYLOAD, "$.response.unsubscribes") AS INT64) AS unsubscribes,
  CAST(JSON_VALUE(PAYLOAD, "$.response.total_bounces") AS INT64) AS total_bounces,
  CAST(JSON_VALUE(PAYLOAD, "$.response.open_rate") AS FLOAT64) AS open_rate,
  CAST(JSON_VALUE(PAYLOAD, "$.response.click_rate") AS FLOAT64) AS click_rate,
  CAST(JSON_VALUE(PAYLOAD, "$.response.unsubscribe_rate") AS FLOAT64) AS unsubscribe_rate,
  CAST(JSON_VALUE(PAYLOAD, "$.response.forward_rate") AS FLOAT64) AS forward_rate,
  CAST(JSON_VALUE(PAYLOAD, "$.response.bounce_rates") AS FLOAT64) AS bounce_rates,
  "Workflows" AS bron
FROM
  ${ref("df_rawdata_views", "csvDataProducer_lasttransaction")}
WHERE
  JSON_VALUE(PAYLOAD, '$.type') = "csvActivecampaignDumpApActivecampaignFlowsPublisher"
    
`
let refs = pk.getRefs()
module.exports = {query, refs}