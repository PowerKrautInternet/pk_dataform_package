/*config*/
let {ref, getRefs} = require("../../sources")
let query = `

SELECT
  "AdPaq" AS bron,
  *

FROM (
  SELECT
    account,
    pk_crm_id,
    date,
    campaign_name,
    creative_name,
    publisher_name,
    device_type,
    instance_name,
    agency_name,
    advertiser_name,
    campaign_id,
    line_item_name,
    station,
    deal_id,
    rollout,
    region_name_1,
    region_name_2,
    postal_code,
    ssp_name,
    currency,
    SUM(impressions) AS impressions,
    SUM(tech_fees) AS tech_fees,
    SUM(agency_fees) AS agency_fees,
    SUM(budget_spent_with_markup) AS budget_spent_with_markup,
    SUM(budget_spent) AS budget_spent

  FROM ${ref("df_rawdata_views", "adpaq_data")}

  GROUP BY
    account,
    pk_crm_id,
    date,
    campaign_name,
    creative_name,
    publisher_name,
    device_type,
    instance_name,
    agency_name,
    advertiser_name,
    campaign_id,
    line_item_name,
    station,
    deal_id,
    rollout,
    region_name_1,
    region_name_2,
    postal_code,
    ssp_name,
    currency
)

`
let refs = getRefs()
module.exports = {query, refs}
