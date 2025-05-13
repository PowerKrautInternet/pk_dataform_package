/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        'GSC' as bron,
        data_date,
        url,
        query,
        country,
        search_type,
        device,
        impressions,
        clicks,
        sum_position,
        sum_position/impressions + 1 AS average_position
    FROM
        ${ref("searchdata_url_impression")}
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}