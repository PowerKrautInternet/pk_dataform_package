/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
JSON_VALUE(PAYLOAD, "$.dtcmedia_crm_id") AS dtcmedia_crm_id,
    JSON_VALUE(PAYLOAD, "$.type") AS type,
    JSON_VALUE(PAYLOAD, "$.name") AS campaign_name,
    PARSE_DATE('%d-%m-%Y', JSON_VALUE(PAYLOAD, "$.date")) AS campaign_date,
    JSON_VALUE(PAYLOAD, "$.response.subject") AS subject_name,
    NULLIF(JSON_VALUE(PAYLOAD, "$.response.workflow"), "") AS workflow,
    JSON_VALUE(PAYLOAD, "$.response.sent_date") AS date_last_sent,
    JSON_VALUE(PAYLOAD, "$.response.date_last_sent_date") AS date_last_sent_date,
    JSON_VALUE(PAYLOAD, "$.response.emails_sent") AS total_sends,
    JSON_VALUE(PAYLOAD, "$.response.unique_opens") AS unique_opens,
    JSON_VALUE(PAYLOAD, "$.response.unique_clicks") AS total_clicks,
    JSON_VALUE(PAYLOAD, "$.response.unsubscribes") AS unsubscribes,
    JSON_VALUE(PAYLOAD, "$.response.bounces") AS total_bounces,
    JSON_VALUE(PAYLOAD, "$.response.open_rate") AS open_rate,
    JSON_VALUE(PAYLOAD, "$.response.click_to_open_ratio") AS click_to_open_ratio,
    JSON_VALUE(PAYLOAD, "$.response.click_rate") AS click_ratio,
    JSON_VALUE(PAYLOAD, "$.response.unsubscribe_rate") AS unsubscribe_rate,
    JSON_VALUE(PAYLOAD, "$.response.forward_rate") AS forward_rate,
    JSON_VALUE(PAYLOAD, "$.response.bounce_rate") AS bounce_rates,
    "eDM" AS bron,
    FROM
${ref("df_rawdata_views", "csvDataProducer_lasttransaction")}
WHERE
JSON_VALUE(PAYLOAD, '$.type') = "csvActivecampaignDumpApActivecampaignEdmPublisher"
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}