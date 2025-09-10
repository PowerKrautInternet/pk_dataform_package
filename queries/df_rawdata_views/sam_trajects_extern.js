
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_VALUE(PAYLOAD, '$.TRAJECTEXTERNGUID') as TRAJECTEXTERNGUID,
        JSON_VALUE(PAYLOAD, '$.TRAJECTID') as TRAJECTEXTERN_TRAJECTID,
        JSON_VALUE(PAYLOAD, '$.SOORTLEADSBRONID') as TRAJECTEXTERN_SOORTLEADSBRONID,
        JSON_VALUE(PAYLOAD, '$.EXTERNGUID') as TRAJECTEXTERN_EXTERNGUID,
        JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') as TRAJECTEXTERN_DTCMEDIA_CRM_ID
FROM (
  SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
    FROM
       ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as first
       WHERE SCHEMA IN (SELECT schema FROM ${ref("df_rawdata_views", "sam_table_hashes")}  WHERE tableName = 'TRAJECTEXTERNGU')
       AND RECEIVEDON = (
          SELECT MAX(RECEIVEDON)
          FROM ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as second
          WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
      GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
