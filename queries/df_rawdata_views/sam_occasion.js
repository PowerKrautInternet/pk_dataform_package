
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_VALUE(PAYLOAD, '$.OCCASIONID') as OCCASION_OCCASIONID,
        JSON_VALUE(PAYLOAD, '$.MERKNAAM') as OCCASION_MERKNAAM,
        JSON_VALUE(PAYLOAD, '$.MODELNAAM') as OCCASION_MODELNAAM,
        JSON_VALUE(PAYLOAD, '$.UITVOERINGNAAM') as OCCASION_UITVOERINGNAAM,
        JSON_VALUE(PAYLOAD, '$.KILOMETERSTAND') as OCCASION_KILOMETERSTAND,
        JSON_VALUE(PAYLOAD, '$.KENTEKEN') as OCCASION_KENTEKEN,
        JSON_VALUE(PAYLOAD, '$.CHASSISNR') as OCCASION_CHASSISNR,
        JSON_VALUE(PAYLOAD, '$.DATUMDEEL1') as OCCASION_DATUMDEEL1,
        JSON_VALUE(PAYLOAD, '$.BOUWJAAR') as OCCASION_BOUWJAAR,
        JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') as OCCASION_DTCMEDIA_CRM_ID
FROM (
    SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
      FROM
         ${ref("samDataProducer_lasttransaction")} as first
         WHERE SCHEMA IN (SELECT schema FROM ${ref("sam_table_hashes")} WHERE tableName = 'OCCASION')
         AND RECEIVEDON = (
            SELECT MAX(RECEIVEDON)
            FROM ${ref("samDataProducer_lasttransaction")} as second
            WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
        GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
