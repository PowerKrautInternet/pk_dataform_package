
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
        JSON_VALUE(PAYLOAD, '$.trajectafsluitredenid') as TRAJECTAFSLUITREDENID,
        JSON_VALUE(PAYLOAD, '$.omschrijving') as TRAJECTAFSLUITREDEN_OMSCHRIJVING,
        JSON_VALUE(PAYLOAD, '$.actief') as TRAJECTAFSLUITREDEN_ACTIEF,
        JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') as TRAJECTAFSLUITREDEN_DTCMEDIA_CRM_ID
        
FROM ${ref("odbcDataProducer_lasttransaction")}
WHERE PUBLISHER = "SAMAfsluitredenPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
