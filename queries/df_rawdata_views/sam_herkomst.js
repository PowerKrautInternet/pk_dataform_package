
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.herkomstid') AS HERKOMST_HERKOMSTID,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS HERKOMST_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.response.omschrijving') AS HERKOMST_OMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.response.systemvalue') AS HERKOMST_SYSTEMVALUE,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS HERKOMST_DTCMEDIA_CRM_ID
FROM 
      ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMHerkomstPublisher"
`
let refs = pk.getRefs()
module.exports = {query, refs}
