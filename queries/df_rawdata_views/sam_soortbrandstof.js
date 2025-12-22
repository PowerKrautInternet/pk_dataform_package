
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.soortbrandstofid') AS SOORTBRANDSTOF_SOORTBRANDSTOFID,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.omschrijving') AS SOORTBRANDSTOF_OMSCHRIJVING,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.response.actief') AS SOORTBRANDSTOF_ACTIEF,
  JSON_EXTRACT_SCALAR(PAYLOAD, '$.dtcmedia_crm_id') AS SOORTBRANDSTOF_DTCMEDIA_CRM_ID
FROM ${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMSoortBrandstofPublisher"
`
let refs = pk.getRefs()
module.exports = {query, refs}
