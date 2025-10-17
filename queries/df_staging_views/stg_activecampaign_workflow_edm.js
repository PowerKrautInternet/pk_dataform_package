/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  IFNULL(workflows.dtcmedia_crm_id, edm.dtcmedia_crm_id) AS dtcmedia_crm_id,
  IFNULL(workflows.type, edm.type) AS type,
  IFNULL(workflows.campaign_date, edm.campaign_date) AS campaign_date,
  campaign_id,
  campaign_name,
  IFNULL(workflows.flow_name, edm.workflow) AS name,
  subject_name,
  date_last_sent_date AS date_last_sent,
  contacts_entered,
  campaigns AS flow_campaigns,
  CAST(NULLIF(edm.total_sends, "") AS INT64) AS total_sends,
  CAST(NULLIF(edm.unique_opens, "") AS INT64) AS unique_opens,
  CAST(NULLIF(edm.total_clicks, "") AS INT64) AS total_clicks,
  CAST(NULLIF(edm.unsubscribes, "") AS INT64) AS unsubscribes,
  CAST(NULLIF(edm.total_bounces, "") AS INT64) AS total_bounces,
  CAST(NULLIF(edm.open_rate, "") AS FLOAT64) AS open_rate,
  CAST(NULLIF(edm.click_ratio, "") AS FLOAT64) AS click_rate,
  CAST(NULLIF(edm.unsubscribe_rate, "") AS FLOAT64) AS unsubscribe_rate,
  CAST(NULLIF(edm.forward_rate, "") AS FLOAT64) AS forward_rate,
  CAST(NULLIF(edm.bounce_rates, "") AS FLOAT64) AS bounce_rates,
  CAST(NULLIF(click_to_open_ratio, "") AS FLOAT64) AS click_to_open_ratio,
  status AS workflow_status,
  IFNULL(workflows.bron, edm.bron) AS bron,
  IFNULL(workflows.account, edm.account) as account,

FROM
  ${ref("df_rawdata_views", "activecampaign_workflows")} workflows
FULL OUTER JOIN
  ${ref("df_rawdata_views", "activecampaign_edm")} edm
ON
  1=0

    
`
let refs = pk.getRefs()
module.exports = {query, refs}