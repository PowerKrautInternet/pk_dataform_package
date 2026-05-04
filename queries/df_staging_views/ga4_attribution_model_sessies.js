/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
SELECT 
event_date,
event_timestamp,
unique_event_id,
event_name,
user_pseudo_id,
event_ga_session_id,
event_ga_session_number,
event_gclid,
event_page_referrer,
event_page_location,
event_source,
event_medium,
traffic_source_source,
traffic_source_medium,
manual_campaign_last_click_source,
manual_campaign_last_click_medium,
cross_channel_campaign_last_click_source,
cross_channel_campaign_last_click_medium,
collected_traffic_source_manual_source,
collected_traffic_source_manual_medium,

FROM ${ref("df_rawdata_views", "ga4_events")} ga4_events

WHERE 
privacy_analytics_storage != 'No'
AND event_name = 'session_start'
AND event_date >= '20240901'
`
let refs = getRefs()
module.exports = {query, refs}
