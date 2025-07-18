/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
    JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
    RECEIVEDON,
    ACTION,
    JSON_VALUE(PAYLOAD, '$.type') AS type,
    JSON_VALUE(PAYLOAD, '$.id') AS id,
    JSON_VALUE(PAYLOAD, '$.response.appName') AS app_name,
    JSON_VALUE(PAYLOAD, '$.response.suppressedMessage') AS suppressed_message,
    JSON_VALUE(PAYLOAD, '$.response.suppressedReason') AS suppressed_reason,
    JSON_VALUE(PAYLOAD, '$.response.dropMessage') AS drop_message,
    JSON_VALUE(PAYLOAD, '$.response.dropReason') AS drop_reason,
    JSON_VALUE(PAYLOAD, '$.response.category') AS response_category,
    JSON_VALUE(PAYLOAD, '$.response.status') AS status,
    JSON_VALUE(PAYLOAD, '$.response.response') AS response,
    JSON_VALUE(PAYLOAD, '$.response.attempt') AS attempt,
    JSON_VALUE(PAYLOAD, '$.response.location.country') AS location_country,
    JSON_VALUE(PAYLOAD, '$.response.location.state') AS location_state,
    JSON_VALUE(PAYLOAD, '$.response.location.city') AS location_city,
    JSON_VALUE(PAYLOAD, '$.response.location.zipcode') AS location_zipcode,
    JSON_VALUE(PAYLOAD, '$.response.browser.name') AS browser_name,
    JSON_VALUE(PAYLOAD, '$.response.browser.family') AS browser_family,
    JSON_VALUE(PAYLOAD, '$.response.browser.type') AS browser_type,
    JSON_VALUE(PAYLOAD, '$.response.duration') AS duration,
    JSON_VALUE(PAYLOAD, '$.response.deviceType') AS device_type,
    JSON_VALUE(PAYLOAD, '$.response.created') AS created,
    JSON_VALUE(PAYLOAD, '$.response.userAgent') AS user_agent,
    JSON_VALUE(PAYLOAD, '$.response.type') AS response_type,
    JSON_VALUE(PAYLOAD, '$.response.subject') AS subject,
    JSON_VALUE(PAYLOAD, '$.response.portalId') AS portalId,
    JSON_VALUE(PAYLOAD, '$.response.recipient') AS recipient,
    JSON_VALUE(PAYLOAD, '$.response.sentBy.id') AS sent_by_id,
    JSON_VALUE(PAYLOAD, '$.response.sentBy.created') AS sent_by_created,
    JSON_VALUE(PAYLOAD, '$.response.smtpId') AS smtp_id,
    JSON_VALUE(PAYLOAD, '$.response.filteredEvent') AS filtered_event,
    JSON_VALUE(PAYLOAD, '$.response.appId') AS app_id,
    JSON_VALUE(PAYLOAD, '$.response.emailCampaignId') AS email_campaign_id,
    JSON_VALUE(PAYLOAD, '$.response.emailCampaignGroupId') AS email_campaign_group_id

FROM
    ${ref("df_rawdata_views", "hubspotExporterDataProducer_lasttransaction")}

WHERE
    JSON_VALUE(PAYLOAD, '$.type') = "HubspotMailEventsPublisher"
AND 
    JSON_VALUE(PAYLOAD, '$.response.source') IS NULL

`
let refs = pk.getRefs()
module.exports = {query, refs}
