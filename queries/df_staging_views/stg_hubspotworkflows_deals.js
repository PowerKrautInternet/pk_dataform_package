/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


SELECT
  CAST(UNIX_MILLIS(TIMESTAMP(DATE(flow_date))) AS STRING) AS datum_bericht,
  msg.object_type,
  msg.workflow_id,
  deals.*,
  contacts.* EXCEPT(hs_object_id,type, pk_crm_id, createdate, lastmodifieddate, email)
FROM
  ${ref("hubspot_bigquerylogging")} msg
LEFT JOIN
  ${ref("hubspot_exported_deals")} deals
ON
  msg.object_id = deals.hs_object_id
LEFT JOIN
  ${ref("hubspot_exported_contacts")} contacts
ON
  msg.contact_object_id = contacts.hs_object_id
WHERE
  msg.object_type = "DEAL"

`
let refs = pk.getRefs()
module.exports = {query, refs}
