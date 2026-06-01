/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') AS type,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.hs_object_id') AS hs_object_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.createdate') AS createdate,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_eind_datum') AS deal_eind_datum,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_imported_from_pk_datalake') AS deal_imported_from_pk_datalake,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_source') AS deal_source,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_start_datum') AS deal_start_datum,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_status') AS deal_status,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_vestiging') AS deal_vestiging,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.deal_id') AS deal_id,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.email') AS email,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.gewenst_merk') AS gewenst_merk,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.gewenst_model') AS gewenst_model,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.hs_lastmodifieddate') AS hs_lastmodifieddate,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.pipeline') AS pipeline
FROM
  ${ref("df_rawdata_views", "hubspot_exporter_data_producer_last_transaction")}
WHERE
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.type') = "HubspotDealsPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
