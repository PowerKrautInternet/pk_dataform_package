/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_VALUE(PAYLOAD, '$.type') AS type,
  JSON_VALUE(PAYLOAD, '$.hs_object_id') AS hs_object_id,
  JSON_VALUE(PAYLOAD, '$.response.createdate') AS createdate,
  JSON_VALUE(PAYLOAD, '$.response.deal_eind_datum') AS deal_eind_datum,
  JSON_VALUE(PAYLOAD, '$.response.deal_imported_from_pk_datalake') AS deal_imported_from_pk_datalake,
  JSON_VALUE(PAYLOAD, '$.response.deal_source') AS deal_source,
  JSON_VALUE(PAYLOAD, '$.response.deal_start_datum') AS deal_start_datum,
  JSON_VALUE(PAYLOAD, '$.response.deal_status') AS deal_status,
  JSON_VALUE(PAYLOAD, '$.response.deal_vestiging') AS deal_vestiging,
  JSON_VALUE(PAYLOAD, '$.response.deal_id') AS deal_id,
  JSON_VALUE(PAYLOAD, '$.response.email') AS email,
  JSON_VALUE(PAYLOAD, '$.response.gewenst_merk') AS gewenst_merk,
  JSON_VALUE(PAYLOAD, '$.response.gewenst_model') AS gewenst_model,
  JSON_VALUE(PAYLOAD, '$.response.hs_lastmodifieddate') AS hs_lastmodifieddate,
  JSON_VALUE(PAYLOAD, '$.response.pipeline') AS pipeline
FROM
  ${ref("df_rawdata_views", "hubspot_exporter_data_producer_last_transaction")}
WHERE
  JSON_VALUE(PAYLOAD, '$.type') = "HubspotDealsPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
