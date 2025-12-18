/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

  SELECT
bron,
dealer,
record_date,
event_name,
unique_event_id,
sessie_user_pseudo_id,
user_pseudo_id,
ga_session_id,
privacy_session_id,
privacy_analytics_storage,
privacy_ads_storage,
session_engaged,
first_user_source,
first_user_medium,
first_user_campaign_name,
session_source,
session_medium,
session_source_medium,
campaign_name,
session_primary_channel_group,
session_default_channel_group,
custom_default_channel_group,
kanaal_groep,
kanaal,
session_device_category,
device_category,
session_device_brand,
session_device_model,
session_device_operating_system,
session_device_is_limited_ad_tracking,
session_device_browser,
session_geo_country,
geo_country,
session_geo_region,
session_geo_city,
session_geo_continent,
conversion_event,
event_buy_status,
--event_formfields_soort,
--event_buy_type

FROM ${ref("dm_benchmarking")} 
WHERE record_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
AND unique_event_id <> ""

`
let refs = pk.getRefs()
module.exports = {query, refs}
