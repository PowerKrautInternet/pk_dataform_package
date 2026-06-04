/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT 
    deal_source,
    deal_id,
    deal_status,
    DATE(TIMESTAMP_MILLIS(deal_start_datum)) AS deal_start_datum,
    DATE(TIMESTAMP_MILLIS(deal_eind_datum)) AS deal_eind_datum,
    deal_afsluit_reden,
    deal_vestiging,
    gewenst_soort_auto,
    gewenst_merk,
    gewenst_model

FROM ${ref("df_datamart_tables", "dm_hubspot_deals")}

WHERE deal_status IN ("Gemiste Lead", "Gemist Salestraject")


`
let refs = pk.getRefs()
module.exports = {query, refs}
