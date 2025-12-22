
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  JSON_VALUE(PAYLOAD, '$.trajectexternguid') AS TRAJECTEXTERNGUID,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS TRAJECTEXTERN_DTCMEDIA_CRM_ID,
  JSON_VALUE(PAYLOAD, '$.type') AS TRAJECT_EXTERN_TYPE,
  JSON_VALUE(PAYLOAD, '$.response.externguid') AS TRAJECTEXTERN_EXTERNGUID,
  JSON_VALUE(PAYLOAD, '$.response.soortleadsbronid') AS TRAJECTEXTERN_SOORTLEADSBRONID,
  JSON_VALUE(PAYLOAD, '$.response.trajectid') AS TRAJECTEXTERN_TRAJECTID

FROM 
${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMTrajectExternGUIDPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
