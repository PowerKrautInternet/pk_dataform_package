
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.afleveringmodelid') AS AFLEVERINGMODEL_AFLEVERINGMODELID,
  JSON_VALUE(PAYLOAD, '$.response.automerkid') AS AFLEVERINGMODEL_AUTOMERKID,
  JSON_VALUE(PAYLOAD, '$.response.omschrijving') AS AFLEVERINGMODEL_OMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS AFLEVERINGMODEL_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS AFLEVERINGMODEL_DTCMEDIA_CRM_ID
  FROM ${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMAfleveringModelPublisher"
AND JSON_VALUE(PAYLOAD, '$.omschrijving') <> ""

`
let refs = pk.getRefs()
module.exports = {query, refs}
