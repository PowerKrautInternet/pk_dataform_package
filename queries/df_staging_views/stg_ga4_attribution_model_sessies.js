/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
SELECT
ga4_events.account AS account,
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
event_campaign,
traffic_source_source,
traffic_source_medium,
traffic_source_name,
manual_campaign_last_click_source,
manual_campaign_last_click_medium,
manual_campaign_last_click_campaign_name,
cross_channel_campaign_last_click_source,
cross_channel_campaign_last_click_medium,
cross_channel_campaign_last_click_campaign_name,
collected_traffic_source_manual_source,
collected_traffic_source_manual_medium,
collected_traffic_source_manual_campaign_name,

FROM ${ref("df_rawdata_views", "ga4_events")} ga4_events

WHERE 
privacy_analytics_storage != 'No'
AND event_name = 'session_start'
`
let refs = getRefs()
module.exports = {query, refs}
