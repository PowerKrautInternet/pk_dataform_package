const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT
email_events.id,
    recipient,
    response_type,
    app_name,
    content_id,
    email_campaigns.subject,
    name AS campaign_name,
    email_type,
app_name,
    attempt,
    location_country,
    location_state,
    location_city,
    browser_name,
    browser_type,
    duration,
    device_type,
    TIMESTAMP_MILLIS(CAST(created AS INT64)) AS created,
    CAST(TIMESTAMP_MILLIS(CAST(created AS INT64)) AS DATE) AS created_date,
    TIMESTAMP_MILLIS(CAST(sent_by_created AS INT64)) AS sent_by_created,
    CAST(TIMESTAMP_MILLIS(CAST(sent_by_created AS INT64)) AS DATE) AS sent_by_created_date,
    EXTRACT(DAYOFWEEK FROM
        TIMESTAMP_MILLIS(CAST(sent_by_created AS INT64))
    ) AS sent_by_created_dayofweek,
    EXTRACT(HOUR FROM
        TIMESTAMP_MILLIS(CAST(sent_by_created AS INT64))
    ) AS sent_by_created_hour,
    user_agent,
    filtered_event,
    email_campaign_id,
    response
FROM
${ref("df_rawdata_views", "hubspot_emailevents")} email_events
LEFT JOIN
${ref("df_rawdata_views", "hubspot_emailcampaigns")} email_campaigns
ON
email_events.email_campaign_id = email_campaigns.id
WHERE
(filtered_event = "false"
OR filtered_event IS NULL)
`
let refs = getRefs()
module.exports = {query, refs}