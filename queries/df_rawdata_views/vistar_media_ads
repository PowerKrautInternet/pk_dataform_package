/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_VALUE(PAYLOAD, '$.type') AS type,
  JSON_VALUE(PAYLOAD, '$.response.year') AS year,
  JSON_VALUE(PAYLOAD, '$.response.quarter') AS quarter,
  JSON_VALUE(PAYLOAD, '$.response.date') AS date,
  JSON_VALUE(PAYLOAD, '$.response.media_owner_display_name') AS media_owner_display_name,
  JSON_VALUE(PAYLOAD, '$.response.venue_type') AS venue_type,
  JSON_VALUE(PAYLOAD, '$.response.advertiser_name') AS advertiser_name,
  JSON_VALUE(PAYLOAD, '$.response.buy_type') AS buy_type,
  JSON_VALUE(PAYLOAD, '$.response.deal') AS deal,
  JSON_VALUE(PAYLOAD, '$.response.deal_id') AS deal_id,
  JSON_VALUE(PAYLOAD, '$.response.insertion_order_name') AS insertion_order_name,
  JSON_VALUE(PAYLOAD, '$.response.insertion_order') AS insertion_order,
  JSON_VALUE(PAYLOAD, '$.response.contract_number') AS contract_number,
  JSON_VALUE(PAYLOAD, '$.response.campaign_name') AS campaign_name,
  JSON_VALUE(PAYLOAD, '$.response.campaign') AS campaign,
  JSON_VALUE(PAYLOAD, '$.response.campaign_id') AS campaign_id,
  JSON_VALUE(PAYLOAD, '$.response.campaign_pixel') AS campaign_pixel,
  JSON_VALUE(PAYLOAD, '$.response.creative_name') AS creative_name,
  JSON_VALUE(PAYLOAD, '$.response.creative') AS creative,
  JSON_VALUE(PAYLOAD, '$.response.creative_id') AS creative_id,
  JSON_VALUE(PAYLOAD, '$.response.creative_pixel') AS creative_pixel,
  JSON_VALUE(PAYLOAD, '$.response.country') AS country,
  JSON_VALUE(PAYLOAD, '$.response.global_city') AS global_city,
  CAST(JSON_VALUE(PAYLOAD, '$.response.impressions') AS FLOAT64) AS impressions,
  CAST(JSON_VALUE(PAYLOAD, '$.response.spots') AS INT64) AS spots,
  CAST(JSON_VALUE(PAYLOAD, '$.response.client_cost') AS FLOAT64) AS client_cost,
  CAST(JSON_VALUE(PAYLOAD, '$.response.client_revenue') AS FLOAT64) AS client_revenue,
  CAST(JSON_VALUE(PAYLOAD, '$.response.client_profit') AS FLOAT64) AS client_profit,
  CAST(JSON_VALUE(PAYLOAD, '$.response.media_cost') AS FLOAT64) AS media_cost,
  CAST(JSON_VALUE(PAYLOAD, '$.response.client_audience_fees') AS FLOAT64) AS client_audience_fees,
  CAST(JSON_VALUE(PAYLOAD, '$.response.client_measurement_fees') AS FLOAT64) AS client_measurement_fees,
  CAST(JSON_VALUE(PAYLOAD, '$.response.media_eCPM') AS FLOAT64) AS media_ecpm,
  CAST(JSON_VALUE(PAYLOAD, '$.response.client_eCPM') AS FLOAT64) AS client_ecpm,
  CAST(JSON_VALUE(PAYLOAD, '$.response.advertiser_eCPM') AS FLOAT64) AS advertiser_ecpm

FROM
    ${ref("vistar_media_dataproducer_lasttransaction")}
WHERE JSON_VALUE(PAYLOAD, '$.type') = "VistarMediaReportPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
