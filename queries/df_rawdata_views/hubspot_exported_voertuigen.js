/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_VALUE(PAYLOAD, '$.type') AS type,
  JSON_VALUE(PAYLOAD, '$.hs_object_id') AS hs_object_id,
  JSON_VALUE(PAYLOAD, '$.objectType') AS objectType,
  JSON_VALUE(PAYLOAD, '$.response.berijder_email') AS berijder_email,
  JSON_VALUE(PAYLOAD, '$.response.voertuig_id') AS voertuig_id,
  JSON_VALUE(PAYLOAD, '$.response.berijder_id') AS berijder_id,
  JSON_VALUE(PAYLOAD, '$.response.hs_createdate') AS hs_createdate,
  JSON_VALUE(PAYLOAD, '$.response.hs_lastmodifieddate') AS hs_lastmodifieddate,
  JSON_VALUE(PAYLOAD, '$.response.kenteken') AS kenteken,
  JSON_VALUE(PAYLOAD, '$.response.merk') AS merk,
  JSON_VALUE(PAYLOAD, '$.response.model') AS model,
  JSON_VALUE(PAYLOAD, '$.response.werkplaats_vestiging') AS werkplaats_vestiging,
  JSON_VALUE(PAYLOAD, '$.response.imported_from_pk_datalake') AS imported_from_pk_datalake
FROM
  ${ref("df_rawdata_views", "hubspot_exporter_data_producer_last_transaction")}
WHERE
  JSON_VALUE(PAYLOAD, '$.type') = "HubspotCustomObjectsPublisherPVoertuigen"


`
let refs = pk.getRefs()
module.exports = {query, refs}
