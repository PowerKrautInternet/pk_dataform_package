/*config*/
let ref = require("../ref.js")
module.exports = `
    
SELECT
    *
FROM
    ${ref("df_staging_views", "stg_ga4_events_sessies")}
    
    `