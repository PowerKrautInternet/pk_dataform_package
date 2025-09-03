
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
config {
    type: "view",
    schema: "df_rawdata_views"
}

SELECT
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.RELATIEOPTINID') as RELATIEOPTIN_RELATIEOPTINID,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.RELATIEID') as RELATIEOPTIN_RELATIEID,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.OPTINSOORT') as RELATIEOPTIN_OPTINSOORT,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.DATUMTIJD') as RELATIEOPTIN_DATUMTIJD,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.BRON') as RELATIEOPTIN_BRON,
        CAST(JSON_EXTRACT_SCALAR(PAYLOAD, '$.OPTIN') AS INT64) as RELATIEOPTIN_OPTIN,
        JSON_EXTRACT_SCALAR(PAYLOAD, '$.DTCMEDIA_CRM_ID') as RELATIEOPTIN_DTCMEDIA_CRM_ID
FROM (
  SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
    FROM
       ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as first
       WHERE SCHEMA IN (SELECT schema FROM ${ref("df_rawdata_views", "samtablehashes")} WHERE tableName = 'RELATIEOPTIN')
       AND RECEIVEDON = (
          SELECT MAX(RECEIVEDON)
          FROM ${ref("df_rawdata_views", "samDataProducer_lasttransaction")} as second
          WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
      GROUP BY PRIMARYFIELDHASH
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
