
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.dealerid') AS DEALER_DEALERID,
  JSON_VALUE(PAYLOAD, '$.response.naam') AS DEALER_NAAM,
  JSON_VALUE(PAYLOAD, '$.response.nummer') AS DEALER_NUMMER,
  JSON_VALUE(PAYLOAD, '$.response.straat') AS DEALER_STRAAT,
  JSON_VALUE(PAYLOAD, '$.response.huisnummer') AS DEALER_HUISNUMMER,
  JSON_VALUE(PAYLOAD, '$.response.postcode') AS DEALER_POSTCODE,
  JSON_VALUE(PAYLOAD, '$.response.plaats') AS DEALER_PLAATS,
  JSON_VALUE(PAYLOAD, '$.response.email') AS DEALER_EMAIL,
  JSON_VALUE(PAYLOAD, '$.response.telefoon1') AS DEALER_TELEFOON1,
  JSON_VALUE(PAYLOAD, '$.response.telefoon3') AS DEALER_TELEFOON_WHATSAPP,
  JSON_VALUE(PAYLOAD, '$.response.banknr') AS DEALER_BANKNR,
  JSON_VALUE(PAYLOAD, '$.response.url') AS DEALER_URL,
  JSON_VALUE(PAYLOAD, '$.response.bronsysteemid') AS DEALER_BRONSYSTEEMID,
  JSON_VALUE(PAYLOAD, '$.response.externid') AS DEALER_EXTERNID,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS DEALER_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS DEALER_DTCMEDIA_CRM_ID
FROM 
${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMDealerPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
