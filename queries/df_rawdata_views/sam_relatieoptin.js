
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_VALUE(PAYLOAD, '$.RELATIEOPTINID') as RELATIEOPTIN_RELATIEOPTINID,
        JSON_VALUE(PAYLOAD, '$.RELATIEID') as RELATIEOPTIN_RELATIEID,
        JSON_VALUE(PAYLOAD, '$.OPTINSOORT') as RELATIEOPTIN_OPTINSOORT,
        JSON_VALUE(PAYLOAD, '$.DATUMTIJD') as RELATIEOPTIN_DATUMTIJD,
        JSON_VALUE(PAYLOAD, '$.BRON') as RELATIEOPTIN_BRON,
        CAST(JSON_VALUE(PAYLOAD, '$.OPTIN') AS INT64) as RELATIEOPTIN_OPTIN,
        JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') as RELATIEOPTIN_DTCMEDIA_CRM_ID
FROM (
  SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
    FROM
       ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as first
       WHERE SCHEMA IN (SELECT schema FROM ${ref("df_rawdata_views", "sam_table_hashes")} WHERE tableName = 'RELATIEOPTIN')
       AND RECEIVEDON = (
          SELECT MAX(RECEIVEDON)
          FROM ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as second
          WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
      GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
