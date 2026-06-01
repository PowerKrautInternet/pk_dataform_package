/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') AS type,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.hs_object_id') AS hs_object_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.city') AS city,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.createdate') AS createdate,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.email') AS email,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.firstname') AS firstname,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.huisnummer') AS huisnummer,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.lastmodifieddate') AS lastmodifieddate,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.lastname') AS lastname,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.zip') AS zip
FROM
  ${ref("df_rawdata_views", "hubspot_exporter_data_producer_last_transaction")}
WHERE
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') ="HubspotContactsPublisher"


`
let refs = pk.getRefs()
module.exports = {query, refs}
