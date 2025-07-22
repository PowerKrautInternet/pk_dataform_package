/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT
* EXCEPT(email_gebounced),
    IF
    (email_afgelevered = 'afgeleverd', 'niet_gebounced', email_gebounced) AS email_gebounced
FROM (
    SELECT
all_events.* EXCEPT(location_city),
    open_events.location_city AS location_city,
    CASE
WHEN open_events.email_geopend = 'geopend' THEN 'geopend'
WHEN open_events.email_geopend IS NULL THEN 'niet_geopend'
ELSE
NULL
END
AS email_geopend,
    open_events.email_geopend_aantal AS email_geopend_aantal,
    CASE
WHEN click_events.email_geklikt = 'geklikt' THEN 'geklikt'
WHEN click_events.email_geklikt IS NULL THEN 'niet_geklikt'
ELSE
NULL
END
AS email_geklikt,
    click_events.email_geklikt_aantal AS email_geklikt_aantal,
    CASE
WHEN processed_events.email_processed = 'processed' THEN 'processed'
WHEN processed_events.email_processed IS NULL THEN 'niet_processed'
ELSE
NULL
END
AS email_verwerkt,
    processed_events.email_processed_aantal AS email_verwerkt_aantal,
    CASE
WHEN delivered_events.email_afgeleverd = 'afgeleverd' THEN 'afgeleverd'
WHEN delivered_events.email_afgeleverd IS NULL THEN 'niet_afgeleverd'
ELSE
NULL
END
AS email_afgelevered,
    delivered_events.email_afgeleverd_aantal AS email_afgeleverd_aantal,
    CASE
WHEN bounced_events.email_bounce = 'gebounced' THEN 'gebounced'
WHEN bounced_events.email_bounce IS NULL THEN 'niet_gebounced'
ELSE
NULL
END
AS email_gebounced,
    bounced_events.email_bounce_aantal AS email_bounce_aantal,
    CASE
WHEN spam_events.email_spam = 'spam' THEN 'spam'
WHEN spam_events.email_spam IS NULL THEN 'niet_spam'
ELSE
NULL
END
AS email_spam,
    spam_events.email_spam_aantal AS email_spam_aantal,
    CASE
WHEN dropped_events.email_gedropped = 'gedropped' THEN 'gedropped'
WHEN dropped_events.email_gedropped IS NULL THEN 'niet_gedropped'
ELSE
NULL
END
AS email_gedropped,
    dropped_events.email_gedropped_aantal AS email_gedropped_aantal,
    'emailstats' AS source,

FROM
${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")} all_events
    
LEFT JOIN
(SELECT
     recipient,
     email_campaign_id,
     sent_by_created,
     MAX(response_type) AS response_type,
     NULLIF(NULLIF(MAX(location_city), ""), "Unknown") AS location_city,
     'geopend' AS email_geopend,
     COUNT(*) AS email_geopend_aantal,
 FROM
     ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE
     response_type = 'OPEN'
 GROUP BY
     recipient,
     email_campaign_id,
     sent_by_created
 ) open_events
ON
all_events.recipient = open_events.recipient
AND all_events.email_campaign_id = open_events.email_campaign_id
AND all_events.sent_by_created = open_events.sent_by_created
    
LEFT JOIN
(SELECT
     recipient,
     email_campaign_id,
     sent_by_created,
     MAX(response_type) AS response_type,
     'geklikt' AS email_geklikt,
     COUNT(*) AS email_geklikt_aantal,
 FROM
     ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE
     response_type = 'CLICK'
 GROUP BY
     recipient,
     email_campaign_id,
     sent_by_created
 ) click_events
ON
all_events.recipient = click_events.recipient
AND all_events.email_campaign_id = click_events.email_campaign_id
AND all_events.sent_by_created = click_events.sent_by_created
    
LEFT JOIN
(SELECT
     recipient,
     email_campaign_id,
     sent_by_created,
     MAX(response_type) AS response_type,
     'processed' AS email_processed,
     COUNT(*) AS email_processed_aantal,
 FROM
     ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE
     response_type = 'PROCESSED'
 GROUP BY
     recipient,
     email_campaign_id,
     sent_by_created
 ) processed_events
ON
all_events.recipient = processed_events.recipient
AND all_events.email_campaign_id = processed_events.email_campaign_id
AND all_events.sent_by_created = processed_events.sent_by_created
    
LEFT JOIN
(SELECT
     recipient,
     email_campaign_id,
     sent_by_created,
     MAX(response_type) AS response_type,
     'afgeleverd' AS email_afgeleverd,
     COUNT(*) AS email_afgeleverd_aantal,
 FROM
     ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE
     response_type = 'DELIVERED'
 GROUP BY
     recipient,
     email_campaign_id,
     sent_by_created
 ) delivered_events
ON
all_events.recipient = delivered_events.recipient
AND all_events.email_campaign_id = delivered_events.email_campaign_id
AND all_events.sent_by_created = delivered_events.sent_by_created
    
LEFT JOIN
(SELECT
     recipient,
     email_campaign_id,
     sent_by_created,
     MAX(response_type) AS response_type,
     'gebounced' AS email_bounce,
     COUNT(*) AS email_bounce_aantal,
 FROM
     ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE
     response_type = 'BOUNCE'
 GROUP BY
     recipient,
     email_campaign_id,
     sent_by_created
 ) bounced_events
ON
all_events.recipient = bounced_events.recipient
AND all_events.email_campaign_id = bounced_events.email_campaign_id
AND all_events.sent_by_created = bounced_events.sent_by_created
    
LEFT JOIN
(SELECT recipient,
        email_campaign_id,
        sent_by_created,
        MAX(response_type) AS response_type,
        'spam'             AS email_spam,
        COUNT(*)           AS email_spam_aantal,
 FROM ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE response_type = 'SPAMREPORT'
 GROUP BY recipient,
          email_campaign_id,
          sent_by_created
 )spam_events
ON
all_events.recipient = spam_events.recipient
AND all_events.email_campaign_id = spam_events.email_campaign_id
AND all_events.sent_by_created = spam_events.sent_by_created
    
LEFT JOIN
(SELECT
     recipient,
     email_campaign_id,
     sent_by_created,
     MAX(response_type) AS response_type,
     'gedropped' AS email_gedropped,
     COUNT(*) AS email_gedropped_aantal,
 FROM
     ${ref("df_staging_views", "stg_hubspot_emailevents_campaigns")}
 WHERE
     response_type = 'DROPPED'
 GROUP BY
     recipient,
     email_campaign_id,
     sent_by_created
 ) dropped_events
ON
all_events.recipient = dropped_events.recipient
AND all_events.email_campaign_id = dropped_events.email_campaign_id
AND all_events.sent_by_created = dropped_events.sent_by_created
WHERE
all_events.response_type = 'SENT')
    
    `
let refs = getRefs()
module.exports = {query, refs}