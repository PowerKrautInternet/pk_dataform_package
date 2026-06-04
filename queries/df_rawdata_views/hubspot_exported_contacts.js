/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_VALUE(PAYLOAD, '$.type') AS type,
  JSON_VALUE(PAYLOAD, '$.hs_object_id') AS hs_object_id,
  JSON_VALUE(PAYLOAD, '$.response.city') AS city,
  JSON_VALUE(PAYLOAD, '$.response.createdate') AS createdate,
  JSON_VALUE(PAYLOAD, '$.response.email') AS email,
  JSON_VALUE(PAYLOAD, '$.response.firstname') AS firstname,
  JSON_VALUE(PAYLOAD, '$.response.huisnummer') AS huisnummer,
  JSON_VALUE(PAYLOAD, '$.response.lastmodifieddate') AS lastmodifieddate,
  JSON_VALUE(PAYLOAD, '$.response.lastname') AS lastname,
  JSON_VALUE(PAYLOAD, '$.response.zip') AS zip
FROM
  ${ref("df_rawdata_views", "hubspot_exporter_data_producer_last_transaction")}
WHERE
  JSON_VALUE(PAYLOAD, '$.type') ="HubspotContactsPublisher"


`
let refs = pk.getRefs()
module.exports = {query, refs}
