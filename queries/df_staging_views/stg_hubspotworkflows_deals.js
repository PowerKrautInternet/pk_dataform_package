/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


SELECT
  CAST(UNIX_MILLIS(TIMESTAMP(DATE(RECEIVEDON))) AS STRING) AS datum_bericht,
  msg.objectType,
  msg.workflowId,
  msg.extra_information,
  deals.*,
  contacts.* EXCEPT(hs_object_id,type, pk_crm_id, createdate, lastmodifieddate, email)
FROM
  ${ref("hubspot_send_message")} msg
LEFT JOIN
  ${ref("hubspot_exported_deals")} deals
ON
  msg.objectId = deals.hs_object_id
LEFT JOIN
  ${ref("hubspot_exported_contacts")} contacts
ON
  msg.contact_hs_object_id = contacts.hs_object_id
WHERE
  msg.objectType = "DEAL"

`
let refs = pk.getRefs()
module.exports = {query, refs}
