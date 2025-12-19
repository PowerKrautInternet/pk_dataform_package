
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.occasionid') AS OCCASION_OCCASIONID,
  JSON_VALUE(PAYLOAD, '$.response.kenteken') AS OCCASION_KENTEKEN,
  JSON_VALUE(PAYLOAD, '$.response.merknaam') AS OCCASION_MERKNAAM,
  JSON_VALUE(PAYLOAD, '$.response.modelnaam') AS OCCASION_MODELNAAM,
  JSON_VALUE(PAYLOAD, '$.response.uitvoeringnaam') AS OCCASION_UITVOERINGNAAM,
  JSON_VALUE(PAYLOAD, '$.response.kilometerstand') AS OCCASION_KILOMETERSTAND,
  JSON_VALUE(PAYLOAD, '$.response.bouwjaar') AS OCCASION_BOUWJAAR,
  JSON_VALUE(PAYLOAD, '$.response.datumdeel1') AS OCCASION_DATUMDEEL1,
  JSON_VALUE(PAYLOAD, '$.response.chassisnr') AS OCCASION_CHASSISNR,
  JSON_VALUE(PAYLOAD, '$.response.kleur') AS OCCASION_KLEUR,
  JSON_VALUE(PAYLOAD, '$.response.brandstof') AS OCCASION_BRANDSTOF,
  JSON_VALUE(PAYLOAD, '$.response.automaat') AS OCCASION_AUTOMAAT,
  JSON_VALUE(PAYLOAD, '$.response.locatie') AS OCCASION_LOCATIE,
  JSON_VALUE(PAYLOAD, '$.response.occasionstatusid') AS OCCASION_STATUSID,
  JSON_VALUE(PAYLOAD, '$.response.verkoopprijs') AS OCCASION_VERKOOPPRIJS,
  JSON_VALUE(PAYLOAD, '$.response.nieuwprijs') AS OCCASION_NIEUWPRIJS,
  JSON_VALUE(PAYLOAD, '$.response.mutatiedatum') AS OCCASION_MUTATIEDATUM,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS OCCASION_DTCMEDIA_CRM_ID
FROM 
      ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMOccasionPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
