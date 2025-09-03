
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT tableName, schema FROM (
  SELECT *, \`pk-datalake-stam\`.rawdata.getTableName(PAYLOAD) as tableName FROM (
    SELECT DISTINCT(schema) as SCHEMA ,MAX(PAYLOAD) as PAYLOAD FROM ${ref("samDataProducer")} GROUP BY schema
  )
) GROUP BY tableName, schema
`
let refs = pk.getRefs()
module.exports = {query, refs}
