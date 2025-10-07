/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT 
"DOOH" AS bron,
*

FROM(
  SELECT
  account,
  advertiser_name,
  media_owner_display_name,
  venue_type,
  buy_type,
  campaign_id,
  insertion_order,
  deal_id,
  creative_id,
  DATE(date) AS date,
  country,
  global_city,
  MAX(deal) as deal,
  MAX(insertion_order_name) AS insertion_order_name,
  MAX(campaign_name) AS campaign_name,
  MAX(creative_name) AS creative_name,
  SUM(CAST(spots AS FLOAT64)) AS spots,
  SUM(CAST(impressions AS FLOAT64)) AS impressions,
  SUM(CAST(client_cost AS FLOAT64)) AS client_cost,
  SUM(CAST(media_cost AS FLOAT64)) AS media_cost
FROM
  ${ref("vistar_media_ads")} placements

GROUP BY
  account,
  advertiser_name,
  media_owner_display_name,
  venue_type,
  buy_type,
  campaign_id,
  insertion_order,
  deal_id,
  creative_id,
  country,
  global_city,
  date)

`
let refs = pk.getRefs()
module.exports = {query, refs}
