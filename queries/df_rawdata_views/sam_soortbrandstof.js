
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.SOORTBRANDSTOFID') as SOORTBRANDSTOF_SOORTBRANDSTOFID,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.OMSCHRIJVING') as SOORTBRANDSTOF_OMSCHRIJVING,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.ACTIEF') as SOORTBRANDSTOF_ACTIEF,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.DTCMEDIA_CRM_ID') as SOORTBRANDSTOF_DTCMEDIA_CRM_ID
FROM (
  SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
    FROM
       ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as first
       WHERE SCHEMA IN (SELECT schema FROM ${ref("df_rawdata_views", "samtablehashes")} WHERE tableName = 'SOORTBRANDSTOF')
       AND RECEIVEDON = (
          SELECT MAX(RECEIVEDON)
          FROM ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as second
          WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
      GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}V
