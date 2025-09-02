
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.GEBRUIKERID') as VERKOPER_GEBRUIKERID,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.NAAM') as VERKOPER_NAAM,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.VOORVOEGSEL') as VERKOPER_VOORVOEGSEL,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.ACTIEF') as VERKOPER_ACTIEF, 
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.SOORTGEBRUIKERID') as VERKOPER_SOORTGEBRUIKERID,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.DTCMEDIA_CRM_ID') as VERKOPER_DTCMEDIA_CRM_ID
FROM (
    SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
        FROM
           ${ref("samDataProducer_lasttransaction")} as first
           WHERE SCHEMA IN (SELECT schema FROM ${ref("df_rawdata_views", "samtablehashes")} WHERE tableName = 'GEBRUIKER')
           AND RECEIVEDON = (
              SELECT MAX(RECEIVEDON)
              FROM ${ref("samDataProducer_lasttransaction")} as second
              WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
          GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
