
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.offertestatusid') AS OFFERTE_STATUS_ID,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS OFFERTE_STATUS_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.response.omschrijving') AS OFFERTE_STATUS_OMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS OFFERTE_STATUS_DTCMEDIA_CRM_ID
FROM
      ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMOfferteStatusPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
