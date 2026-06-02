/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


SELECT
  CAST(UNIX_MILLIS(TIMESTAMP(DATE(flow_date))) AS STRING) AS datum_bericht,
  email,
  msg.object_type,
  msg.workflow_id,
  msg.extra_information,
  voertuigen.* EXCEPT(objectType),
  contacts.* EXCEPT(hs_object_id,pk_crm_id, type, createdate, lastmodifieddate, email)
FROM
  ${ref("hubspot_bigquerylogging")} msg
LEFT JOIN
  ${ref("hubspot_exported_voertuigen")} voertuigen
ON
  msg.object_id = voertuigen.hs_object_id
LEFT JOIN
  ${ref("hubspot_exported_contacts")} contacts
ON
  msg.contact_object_id = contacts.hs_object_id
WHERE
  msg.object_type = voertuigen.objectType

`
let refs = pk.getRefs()
module.exports = {query, refs}
