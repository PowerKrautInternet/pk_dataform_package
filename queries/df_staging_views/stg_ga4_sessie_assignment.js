/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
    
SELECT
    account,
    user_pseudo_id,
    event_ga_session_id as ga_session_id,
    event_ga_session_number as ga_session_number,
    event_page_referrer as session_landingpage_referrer,
    event_page_location as session_landingpage_location, -- merk
    event_page_title as session_landingpage_title, -- merk
    event_buy_status, 
    event_buy_model, 
    event_buy_plaats, 
    event_buy_brand, 
    event_trade_in_model, 
    event_trade_in_brand,
    event_formfields_id,
    event_formfields_merk,
    event_formfields_model,
    event_formfields_soort,
    event_formfields_titel,
    event_formfields_vestiging,
    IFNULL(event_engaged_session_event, 0) AS session_engaged,
    --event_engagement_time_msec,
    -- event_session_engaged
    -- event_engaged_session_event
    user_first_touch_timestamp,
    is_active_user,
    --privacy_analytics_storage,
    --privacy_ads_storage,
    
    collected_traffic_source_gclid,
    IF(collected_traffic_source_gclid <> "", cross_channel_campaign_last_click_source, 
    IFNULL(collected_traffic_source_manual_source, cross_channel_campaign_last_click_source)) AS session_source,
    collected_traffic_source_manual_source,
    cross_channel_campaign_last_click_source,
    IF(google_ads_campaign_last_click_customer_id <> "", 'google', NULL) AS google_ads_source,
    manual_campaign_last_click_source,
    first_user_source,

    IF(collected_traffic_source_gclid <> "", cross_channel_campaign_last_click_medium, 
    IFNULL(collected_traffic_source_manual_medium, cross_channel_campaign_last_click_medium)) AS session_medium,
    collected_traffic_source_manual_medium,
    cross_channel_campaign_last_click_medium,
    IF(google_ads_campaign_last_click_customer_id <> "", 'cpc', NULL) AS google_ads_medium,
    manual_campaign_last_click_medium,
    first_user_medium,

    IF(collected_traffic_source_gclid <> "", cross_channel_campaign_last_click_campaign_name, 
    IFNULL(collected_traffic_source_manual_campaign_name, cross_channel_campaign_last_click_campaign_name)) AS session_campaign,
    collected_traffic_source_manual_campaign_name,
    cross_channel_campaign_last_click_campaign_name,
    google_ads_campaign_last_click_campaign_name,
    manual_campaign_last_click_campaign_name,
    first_user_campaign_name,
    
    cross_channel_campaign_last_click_default_channel_group as session_default_channel_group,
    cross_channel_campaign_last_click_primary_channel_group as session_primary_channel_group,
    IFNULL(collected_traffic_source_manual_term, manual_campaign_last_click_term) AS session_term,
    IFNULL(collected_traffic_source_manual_content, manual_campaign_last_click_content) AS session_content,
    cross_channel_campaign_last_click_campaign_id as session_campaign_id,
    google_ads_campaign_last_click_customer_id as session_google_ads_customer_id,
    google_ads_campaign_last_click_account_name as session_google_ads_account_name,
    google_ads_campaign_last_click_ad_group_id as session_google_ads_ad_group_id,
    google_ads_campaign_last_click_ad_group_name as session_google_ads_ad_group_name,
    device_category as session_device_category,
    device_mobile_brand_name as session_device_brand,
    IFNULL(device_mobile_marketing_name, device_mobile_model_name) AS session_device_model,
    device_operating_system as session_device_operating_system,
    device_is_limited_ad_tracking as session_device_is_limited_ad_tracking,
    device_webinfo_browser as session_device_browser,
    geo_city as session_geo_city,
    geo_country as session_geo_country,
    geo_region as session_geo_region,
    geo_continent as session_geo_continent

FROM(
SELECT 
    account,
    event_date,
    unique_event_id,
    event_timestamp,
    event_name,
    user_pseudo_id,
    event_bundle_sequence_id,
    event_ga_session_id,
    event_ga_session_number,
    event_gclid,
    event_page_referrer,
    event_page_location,
    event_page_title,
    event_term,
    event_engagement_time_msec,
    event_entrances,
    event_session_engaged,
    event_engaged_session_event,
    event_buy_status, 
    event_buy_model, 
    event_buy_plaats, 
    event_buy_brand, 
    event_trade_in_model, 
    event_trade_in_brand, 
    privacy_analytics_storage,
    privacy_ads_storage,
    user_first_touch_timestamp,
    event_source, -- utm source event
    event_medium, -- utm source event
    event_campaign_id, -- utm campaign id
    event_campaign, -- utm campaign
    traffic_source_name as first_user_campaign_name,
    traffic_source_medium as first_user_medium,
    traffic_source_source as first_user_source,
    -- contains the traffic source data that was collected with the first event of the page
    collected_traffic_source_manual_campaign_name, -- The manual campaign name (utm_campaign) that was collected with the event.
    collected_traffic_source_manual_source, -- The manual campaign source (utm_source) that was collected with the event. Also includes parsed parameters from referral params, not just UTM values.
    collected_traffic_source_manual_medium, -- The manual campaign medium (utm_medium) that was collected with the event. Also includes parsed parameters from referral params, not just UTM values.
    collected_traffic_source_gclid, -- The Google click identifier that was collected with the event.
    collected_traffic_source_manual_term,
    collected_traffic_source_manual_content,
    is_active_user,
     -- The session_traffic_source_last_click RECORD contains the last-click attributed session traffic source data across Google ads and manual contexts, where available.
    manual_campaign_last_click_campaign_id,
    manual_campaign_last_click_campaign_name,
    manual_campaign_last_click_source,
    manual_campaign_last_click_medium,
    manual_campaign_last_click_term,
    manual_campaign_last_click_content, -- ifnull met term
    google_ads_campaign_last_click_customer_id,
    google_ads_campaign_last_click_account_name,
    google_ads_campaign_last_click_campaign_id,
    google_ads_campaign_last_click_campaign_name,
    google_ads_campaign_last_click_ad_group_id,
    google_ads_campaign_last_click_ad_group_name,
    cross_channel_campaign_last_click_campaign_id,
    cross_channel_campaign_last_click_campaign_name,
    cross_channel_campaign_last_click_source,
    cross_channel_campaign_last_click_medium,
    cross_channel_campaign_last_click_source_platform,
    cross_channel_campaign_last_click_default_channel_group,
    cross_channel_campaign_last_click_primary_channel_group,
    device_category,
    device_mobile_brand_name,
    device_mobile_model_name,
    device_mobile_marketing_name,
    device_operating_system,
    device_is_limited_ad_tracking,
    device_webinfo_browser,
    geo_city,
    geo_country,
    geo_region,
    geo_continent,
    event_formfields_id,
    event_formfields_merk,
    event_formfields_model,
    event_formfields_soort,
    event_formfields_titel,
    event_formfields_vestiging,

FROM ${ref("ga4_events")}

WHERE user_pseudo_id <> "" AND CAST(event_ga_session_id AS STRING) <> ""
AND event_name = 'session_start'
)

`
let refs = pk.getRefs()
module.exports = {query, refs}
