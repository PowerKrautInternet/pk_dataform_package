
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS RELATIEOPTIN_DTCMEDIA_CRM_ID,
  JSON_VALUE(PAYLOAD, '$.type') AS RELATIEOPTIN_TYPE,
  JSON_VALUE(PAYLOAD, '$.relatieoptinid') AS RELATIEOPTIN_ID,
  JSON_VALUE(PAYLOAD, '$.response.bron') AS RELATIEOPTIN_BRON,
  JSON_VALUE(PAYLOAD, '$.response.datumtijd') AS RELATIEOPTIN_DATUMTIJD,
  JSON_VALUE(PAYLOAD, '$.response.gebruikerid') AS RELATIEOPTIN_GEBRUIKERID,
  JSON_VALUE(PAYLOAD, '$.response.optin') AS RELATIEOPTIN_STATUS,
  JSON_VALUE(PAYLOAD, '$.response.optinsoort') AS RELATIEOPTIN_SOORT,
  JSON_VALUE(PAYLOAD, '$.response.relatieid') AS RELATIEOPTIN_RELATIEID,
  JSON_VALUE(PAYLOAD, '$.response.vorigebron') AS RELATIEOPTIN_VORIGEBRON,
  JSON_VALUE(PAYLOAD, '$.response.vorigedatumtijd') AS RELATIEOPTIN_VORIGEDATUMTIJD,
  JSON_VALUE(PAYLOAD, '$.response.vorigegebruikerid') AS RELATIEOPTIN_VORIGEGEBRUIKERID,
  JSON_VALUE(PAYLOAD, '$.response.vorigeoptin') AS RELATIEOPTIN_VORIGEOPTIN
FROM ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMRelatieOptinPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
