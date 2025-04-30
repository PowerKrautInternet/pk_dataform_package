/*config*/
let pk = require("../ref.js")
let ref = pk.ref
module.exports = `
    
SELECT
    *
FROM
    ${ref("df_staging_views", "stg_ga4_events_sessies")}
    
`
let refs = pk.getRefs()
module.exports = {query, refs}