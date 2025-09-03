
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_VALUE(PAYLOAD, '$.DEALERID') as DEALER_DEALERID,
        JSON_VALUE(PAYLOAD, '$.NAAM') as DEALER_NAAM,
        JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') as DEALER_DTCMEDIA_CRM_ID
FROM (
    SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
      FROM
         ${ref("samDataProducer_lasttransaction")} as first
         WHERE SCHEMA IN (SELECT schema FROM ${ref("sam_table_hashes")} WHERE tableName = 'DEALER')
         AND RECEIVEDON = (
            SELECT MAX(RECEIVEDON)
            FROM ${ref("samDataProducer_lasttransaction")} as second
            WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
        GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
