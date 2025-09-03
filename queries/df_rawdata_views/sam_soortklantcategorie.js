
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT 
JSON_EXTRACT_SCALAR(PAYLOAD, '$.KLANTCATEGORIEID') AS SOORTKLANTCATEGORIEID,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.OMSCHRIJVING') AS SOORTKLANTCATEGORIE_OMSCHRIJVING,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.ACTIEF') AS SOORTKLANTCATEGORIE_ACTIEF,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.EXTERNID') AS SOORTKLANTCATEGORIE_EXTERNID,
JSON_EXTRACT_SCALAR(PAYLOAD, '$.DTCMEDIA_CRM_ID') AS SOORTKLANTCATEGORIE_DTCMEDIA_CRM_ID
        
FROM (
    SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
    FROM
      ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as first
     WHERE SCHEMA IN (SELECT schema FROM ${ref("df_rawdata_views", "samtablehashes")} WHERE tableName = 'KLANTCATEGORIE')
     AND RECEIVEDON = (
        SELECT MAX(RECEIVEDON)
        FROM ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as second
        WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
    GROUP BY PRIMARYFIELDHASH
    )

`
let refs = pk.getRefs()
module.exports = {query, refs}
