/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
    account,
    CONCAT(IFNULL(user_pseudo_id, ""), "_", CAST(event_timestamp AS STRING), "_", event_name) AS unique_event_id,
    item_id,
    item_name,
    item_brand,
    item_variant,
    item_category,
    price as item_price,
    quantity as item_quantity, 
    item_revenue,
    affiliation as item_affiliation,
    item_list_name,
    (SELECT value.string_value FROM UNNEST(item_params) WHERE key = 'item_aanvraagformulier') AS item_aanvraagformulier,
    (SELECT value.string_value FROM UNNEST(item_params) WHERE key = 'item_aanvraagtype') AS item_aanvraagtype,
    (SELECT value.string_value FROM UNNEST(item_params) WHERE key = 'item_afleverpakket') AS item_afleverpakket

FROM ${ref("events_*")},
UNNEST(items) AS items
`
let refs = pk.getRefs()
module.exports = {query, refs}
