
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.automerkid') AS AUTOMERK_AUTOMERKID,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS AUTOMERK_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.response.autodatasourceid') AS AUTOMERK_AUTODATASOURCEID,
  JSON_VALUE(PAYLOAD, '$.response.dataversion') AS AUTOMERK_DATAVERSION,
  JSON_VALUE(PAYLOAD, '$.response.imageversion') AS AUTOMERK_IMAGEVERSION,
  JSON_VALUE(PAYLOAD, '$.response.imgmake') AS AUTOMERK_IMGMAKE,
  JSON_VALUE(PAYLOAD, '$.response.jatowltp') AS AUTOMERK_JATOWLTP,
  JSON_VALUE(PAYLOAD, '$.response.omschrijving') AS AUTOMERK_OMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS AUTOMERK_DTCMEDIA_CRM_ID
FROM ${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMAutoMerkPublisher"
AND JSON_VALUE(PAYLOAD, '$.omschrijving') <> ""

`
let refs = pk.getRefs()
module.exports = {query, refs}
