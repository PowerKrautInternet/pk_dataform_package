
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_VALUE(PAYLOAD, '$.AFLEVERINGMODELID') as AFLEVERINGMODEL_AFLEVERINGMODELID,
        JSON_VALUE(PAYLOAD, '$.AUTOMERKID') as AFLEVERINGMODEL_AUTOMERKID,
        JSON_VALUE(PAYLOAD, '$.OMSCHRIJVING') as AFLEVERINGMODEL_OMSCHRIJVING,
        --JSON_VALUE(PAYLOAD, '$.ACTIEF') as AFLEVERINGMODEL_ACTIEF,
        JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') as AFLEVERINGMODEL_DTCMEDIA_CRM_ID
  FROM (
    SELECT MAX(SCHEMA), PRIMARYFIELDHASH, MAX(PAYLOAD) as PAYLOAD, MAX(ACTION) as ACTION, MAX(RECEIVEDON) as RECEIVEDON
        FROM
           ${ref("samDataProducer_lasttransaction")} as first
           WHERE SCHEMA IN (SELECT schema FROM ${ref("sam_table_hashes")} WHERE tableName = 'AFLEVERINGMODEL')
           AND RECEIVEDON = (
              SELECT MAX(RECEIVEDON)
              FROM ${ref("samDataProducer_lasttransaction")} as second
              WHERE first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH and first.SCHEMA = second.SCHEMA)
          GROUP BY PRIMARYFIELDHASH
)

WHERE JSON_VALUE(PAYLOAD, '$.OMSCHRIJVING') <> ""

`
let refs = pk.getRefs()
module.exports = {query, refs}
