/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


SELECT
  CAST(UNIX_MILLIS(TIMESTAMP(DATE(RECEIVEDON))) AS STRING) AS datum_bericht,
  email,
  msg.objectType,
  msg.workflowId,
  msg.extra_information,
  voertuigen.* EXCEPT(objectType),
  contacts.* EXCEPT(hs_object_id,pk_crm_id, type, createdate, lastmodifieddate, email)
FROM
  ${ref("hubspot_bigquerylogging")} msg
LEFT JOIN
  ${ref("hubspot_exported_voertuigen")} voertuigen
ON
  msg.objectId = voertuigen.hs_object_id
LEFT JOIN
  ${ref("hubspot_exported_contacts")} contacts
ON
  msg.contact_hs_object_id = contacts.hs_object_id
WHERE
  msg.objectType = "2-131617677"

`
let refs = pk.getRefs()
module.exports = {query, refs}
