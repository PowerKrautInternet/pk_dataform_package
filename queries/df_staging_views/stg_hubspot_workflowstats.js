/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
    
SELECT
    hubspot.*,
    ${ifSource(["hubspot_bigquerylogging", "gs_mapping_edmworkflow"], `
        IFNULL(hubspot.hs_workflow_name, mapping.workflow_mapping) AS hs_workflow_name,
    `)}
    ${ifSource("gs_mapping_edmworkflow_campagne", `
        campagne.campaign AS hs_campaign,
        campagne.edm AS hs_edm,
        campagne.workflow AS hs_workflow,
        campagne.edmworkflow AS type,
    `)}

FROM (
    SELECT
        ${ifNull([
            "email.sent_by_created_date",
            ifSource("hubspot_bigquerylogging", "flows.flow_date"),
            ifSource("stg_hubspot_contact_count", "aantal.RECEIVEDON")    
        ], "AS hs_date,")}
        ${ifNull([
            "email.source",
            ifSource("hubspot_bigquerylogging", "flows.source"),    
        ], "AS hs_source,")}
        'Hubspot' AS hs_bron,
        flows.workflow_name as hs_workflow_name,
        email.id as hs_email_id,
        email.recipient AS hs_recipient,
        email.response_type AS hs_response_type,
        email.app_name AS hs_app_name,
        email.content_id AS hs_content_id,
        email.subject AS hs_subject,
        email.campaign_name AS edm_name,
        email.email_type AS hs_email_type,
        email.app_name AS hs_appName,
        email.attempt AS hs_attempt,
        email.location_country AS hs_location_country,
        email.location_state AS hs_location_state,
        email.browser_name AS hs_browser_name,
        email.browser_type AS hs_browser_type,
        email.duration AS hs_duration,
        email.deviceType AS hs_deviceType,
        email.created AS hs_created,
        email.sent_by_created_dayofweek AS hs_sent_by_created_dayofweek,
        email.sent_by_created_hour AS hs_sent_by_created_hour,
        email.user_agent AS hs_user_agent,
        email.filtered_event AS hs_filtered_event,
        email.email_campaign_id AS hs_email_campaignId,
        email.response AS hs_response,
        email.location_city AS hs_location_city,
        email.email_geopend_aantal AS hs_email_geopend_aantal,
        email.email_geklikt_aantal AS hs_email_geklikt_aantal,
        email.email_verwerkt_aantal AS hs_email_verwerkt_aantal,
        email.email_afgeleverd_aantal AS hs_email_afgeleverd_aantal,
        email.email_bounce_aantal AS hs_email_bounce_aantal,
        email.email_spam_aantal AS hs_email_spam_aantal,
        email.email_gedropped_aantal AS hs_email_gedropped_aantal,
        email.email_gebounced AS hs_email_gebounced,
        IF(email.email_geopend = 'geopend', 1,0) AS email_geopend,
        IF(email.email_geklikt = 'geklikt', 1,0) AS email_geklikt,
        IF(email.email_verwerkt = 'processed', 1,0) AS email_verwerkt,
        IF(email.email_afgelevered = 'afgeleverd', 1,0) AS email_afgelevered,
        IF(email.email_gebounced = 'gebounced', 1,0) AS email_gebounced,
        IF(email.email_spam = 'spam', 1,0) AS email_spam,
        IF(email.email_gedropped = 'gedropped', 1,0) AS email_gedropped,
        ${ifSource("hubspot_bigquerylogging",`
            flows.hs_callback_id,
            flows.callback_id AS hs_callback_id,
            flows.portal_id AS hs_portal_id,
            flows.user_id AS hs_user_id,
            flows.action_definition_id AS hs_action_definition_id,
            flows.action_definition_version AS hs_action_definition_version,
            flows.origin_enrollment_id AS hs_origin_enrollment_id,
            flows.origin_action_execution_index AS hs_origin_action_execution_index,
            flows.extension_definition_version_id AS hs_extension_definition_version_id,
            flows.extension_definition_id AS hs_extension_definition_id,
            flows.workflowId AS hs_workflow_id,
            flows.action_id AS hs_action_id,
            flows.context_enrollment_id AS hs_context_enrollment_id,
            flows.context_action_execution_index AS hs_context_action_execution_index,
            flows.object_id AS hs_object_id,
            flows.object_type AS hs_object_type,
            flows.fields_associate_contact AS hs_fields_associate_contact,
            flows.input_fields_associate_contact AS hs_input_fields_associate_contact,
            flows.source_flow AS hs_source_flow,
        `)}
        ${ifSource("stg_hubspot_contact_count", `
            aantal.count AS hs_count,
            aantal.RECEIVEDON AS hs_RECEIVEDON,
        `)}

    FROM ${ref("df_datamart_views", "dm_hubspot_emailstats")} email 
        ${join("FULL OUTER JOIN", "df_rawdata_views", "hubspot_bigquerylogging", "AS flows ON 1=0")} 
        ${join("FULL OUTER JOIN", "df_rawdata_tables", "stg_hubspot_contacts_count", "AS aantal ON 1=0")}
) hubspot
${join("LEFT JOIN", "googleSheets", "gs_mapping_edmworkflow_campagne", `AS campagne ON hubspot.hs_campaign_name = campagne.edm ${ifSource("hubspot_bigquerylogging", "OR hubspot.hs_workflow_name = campagne.workflow")}`)}
${join("LEFT JOIN", "googleSheets", "gs_mapping_edmworkflow", "AS mapping ON hubspot.hs_campaign_name = mapping.edm")}
`
let refs = getRefs()
module.exports = {query, refs}