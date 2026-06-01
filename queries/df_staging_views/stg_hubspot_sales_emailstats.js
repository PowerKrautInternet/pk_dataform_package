/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  CAST(TIMESTAMP_MILLIS(CAST(datum_bericht AS INT64)) AS DATE) AS datum_bericht,
  ifnull(verstuurd_bericht, msg.workflow_id) as verstuurd_bericht,
  email,
  dealid,
  gewenst_merk,
  gewenst_model,
  msg.workflow_id,
  
  FROM
  (
    SELECT 
      COALESCE(deals.datum_bericht, voertuigen.datum_bericht) AS datum_bericht,
      COALESCE(deals.email, voertuigen.email) AS email,
      COALESCE(deals.workflowId, voertuigen.workflowId) AS workflow_id,
      deal_id,
      gewenst_merk,
      gewenst_model,

      FROM
      ${ref("stg_hubspotworkflows_deals")} deals
      FULL OUTER JOIN
      ${ref("stg_hubspotworkflows_voertuigen")} voertuigen
      ON 1=0 
    ) msg 
LEFT JOIN
  ${ref("googleSheets","gs_workflows_dashboarding")} flows
ON
  msg.workflow_id = flows.workflow_id
WHERE msg.workflow_id <> ""

`
let refs = pk.getRefs()
module.exports = {query, refs}
