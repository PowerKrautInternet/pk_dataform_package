/*config*/
let pk = require("../../setup")
let ref = pk.ref
let query = `

SELECT
  'Facebook' as bron,
  pk_crm_id,
  account_id,
  ad_id,
  adset_id,
  campaign_id,
  CAST(date_start AS DATE) AS date_start,
  MAX(account_name) AS account_name,
  MAX(campaign_name) AS campaign_name,
  MAX(ad_name) AS ad_name,
  MAX(adset_name) AS adset_name,
  MAX(CAST(reach AS INT64)) AS reach,
  MAX(CAST(impressions AS INT64)) AS impressions,
  MAX(CAST(clicks AS INT64)) AS clicks,
  MAX(CAST(conversions AS INT64)) AS conversions,
  MAX(CAST(spend AS FLOAT64)) AS spend,
  MAX(date_stop) AS date_stop,
  MAX(objective) AS objective,
  MAX(
  IF(JSON_VALUE(actions_nieuw,'$.action_type') = "page_engagement", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), 0)
    ) AS page_engagement,
  MAX(
  IF
    (JSON_VALUE(actions_nieuw,'$.action_type') = "post_engagement", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), NULL)) AS post_engagement,
  MAX(
  IF
    (JSON_VALUE(actions_nieuw,'$.action_type') = "lead", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), NULL)) AS lead,
  MAX(
  IF
    (JSON_VALUE(actions_nieuw,'$.action_type') = "leadgen_grouped", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), NULL)) AS leadgen_grouped,
  MAX(
  IF
    (JSON_VALUE(actions_nieuw,'$.action_type') = "onsite_conversion.lead_grouped", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), NULL)) AS onsite_lead_grouped,
  MAX(
  IF
    (JSON_VALUE(actions_nieuw,'$.action_type') = "post_reaction", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), NULL)) AS post_reaction,
  MAX(
  IF
    (JSON_VALUE(actions_nieuw,'$.action_type') = "link_click", CAST(JSON_VALUE(actions_nieuw,'$.value') AS INT64), NULL)) AS link_click
FROM
    ${ref("df_rawdata_views", "facebookdata")}

GROUP BY
  pk_crm_id,
  account_id,
  ad_id,
  adset_id,
  campaign_id,
  date_start

`
let refs = pk.getRefs()
module.exports = {query, refs}