/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
        
SELECT
    JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
    JSON_VALUE(PAYLOAD, '$.type') AS type,
    JSON_VALUE(PAYLOAD, '$.id') AS id,
    JSON_VALUE(PAYLOAD, '$.response.appId') AS app_id,
    JSON_VALUE(PAYLOAD, '$.response.appName') AS app_name,
    JSON_VALUE(PAYLOAD, '$.response.contentId') AS content_id,
    JSON_VALUE(PAYLOAD, '$.response.subject') AS subject,
    JSON_VALUE(PAYLOAD, '$.response.name') AS name,
    JSON_VALUE(PAYLOAD, '$.response.counters.deferred') AS deffered,
    JSON_VALUE(PAYLOAD, '$.response.counters.bounce') AS bounce,
    JSON_VALUE(PAYLOAD, '$.response.counters.dropped') AS dropped,
    JSON_VALUE(PAYLOAD, '$.response.counters.delivered') AS delivered,
    JSON_VALUE(PAYLOAD, '$.response.counters.sent') AS sent,
    JSON_VALUE(PAYLOAD, '$.response.counters.click') AS click,
    JSON_VALUE(PAYLOAD, '$.response.counters.spamreport') AS spam_report,
    JSON_VALUE(PAYLOAD, '$.response.counters.processed') AS processed,
    JSON_VALUE(PAYLOAD, '$.response.counters.unsubscribed') AS unsubscribed,
    JSON_VALUE(PAYLOAD, '$.response.counters.statuschange') AS status_change,
    JSON_VALUE(PAYLOAD, '$.response.counters.mta_dropped') AS mta_dropped,
    JSON_VALUE(PAYLOAD, '$.response.counters.suppressed') AS suppressed,
    JSON_VALUE(PAYLOAD, '$.response.counters.open') AS open,
    JSON_VALUE(PAYLOAD, '$.response.lastProcessingFinishedAt') AS last_proecessing_finished_at,
    JSON_VALUE(PAYLOAD, '$.response.lastProcessingStartedAt') AS last_processing_started_at,
    JSON_VALUE(PAYLOAD, '$.response.lastProcessingStateChangeAt') AS last_processing_state_change_at,
    JSON_VALUE(PAYLOAD, '$.response.numIncluded') AS num_included,
    JSON_VALUE(PAYLOAD, '$.response.processingState') AS processing_state,
    JSON_VALUE(PAYLOAD, '$.response.scheduledAt') AS scheduled_at,
    JSON_VALUE(PAYLOAD, '$.response.type') AS email_type,
    
FROM
    ${ref("df_rawdata_views", "hubspotExporterDataProducer_lasttransaction")}

WHERE
    JSON_VALUE(PAYLOAD, '$.type') = "HubspotMailCampaignsPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
