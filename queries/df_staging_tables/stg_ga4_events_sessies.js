/*config*/
let pk = require("../../functions")
let ref = pk.ref
let query = `
    
SELECT
    *
FROM
    ${ref("df_staging_views", "stg_ga4_events_sessies")}
    
`
let refs = pk.getRefs()
module.exports = {query, refs}