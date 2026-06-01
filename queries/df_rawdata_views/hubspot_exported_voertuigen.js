/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') AS type,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.hs_object_id') AS hs_object_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.objectType') AS objectType,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.berijder_email') AS berijder_email,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.berijder_id') AS berijder_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.hs_createdate') AS hs_createdate,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.hs_lastmodifieddate') AS hs_lastmodifieddate,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.kenteken') AS kenteken,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.merk') AS merk,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.voertuig_imported_from_pk_datalake') AS voertuig_imported_from_pk_datalake
FROM
  ${ref("df_rawdata_views", "hubspot_exporter_data_producer_last_transaction")}
WHERE
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') = "HubspotCustomObjectsPublisherPVoertuigen"


`
let refs = pk.getRefs()
module.exports = {query, refs}
