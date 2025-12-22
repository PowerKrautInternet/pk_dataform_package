
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT 
  JSON_VALUE(PAYLOAD, '$.klantcategorieid') AS SOORTKLANTCATEGORIEID,
  JSON_VALUE(PAYLOAD, '$.response.omschrijving') AS SOORTKLANTCATEGORIE_OMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS SOORTKLANTCATEGORIE_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.response.externid') AS SOORTKLANTCATEGORIE_EXTERNID,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS SOORTKLANTCATEGORIE_DTCMEDIA_CRM_ID
FROM ${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMSoortKlantcategoriePublisher"
`
let refs = pk.getRefs()
module.exports = {query, refs}
