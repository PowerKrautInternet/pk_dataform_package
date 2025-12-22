
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.offertestatusid') AS OFFERTESTATUS_OFFERTESTATUSID,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS OFFERTESTATUS_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.response.omschrijving') AS OFFERTESTATUS_OMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS OFFERTESTATUS_DTCMEDIA_CRM_ID
FROM
      ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMOfferteStatusPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
