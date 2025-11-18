/*config*/
let {ref, getRefs, join, ifNull, ifSource, orSource} = require("../../sources")
let query = `

SELECT
* ${ifSource("gs_merken", `EXCEPT(merk_event, merk_session),
    IFNULL(merk_event, merk_session) AS merk_event,
    IFNULL(merk_session, merk_event) AS merk_session`)},
    IFNULL(IFNULL(NULLIF(session_default_channel_group, 'Unassigned'), custom_default_channel_group), 'Unassigned') as kanaal

FROM(
    SELECT
    * EXCEPT(session_default_channel_group),
    ${ifSource("gs_merken", `${ref("lookupTable")}(
        event_merk_concat,
        TO_JSON_STRING(ARRAY(SELECT merk FROM ${ref("df_googlesheets_tables","gs_merken", true)}))
    ) as merk_event,
    ${ref("lookupTable")}(session_merk_concat,
        TO_JSON_STRING(ARRAY(SELECT merk FROM ${ref("df_googlesheets_tables","gs_merken", true)}))) as merk_session,`)}
    session_default_channel_group,
    CASE
    WHEN regexp_contains(LOWER(session_medium),'whatsapp') THEN 'Whatsapp'
    WHEN session_source = '(direct)' AND (session_medium IN ('(not set)', '(none)') OR session_medium IS NULL) THEN 'Direct'
    WHEN REGEXP_CONTAINS(session_campaign, 'cross-network') THEN 'Cross-network'
    WHEN (REGEXP_CONTAINS(session_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') OR REGEXP_CONTAINS(session_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$')) AND REGEXP_CONTAINS(session_medium, '^(.*cp.*|ppc|paid.*)$') THEN 'Paid Shopping'
    WHEN REGEXP_CONTAINS(session_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex|adwords')
    AND REGEXP_CONTAINS(session_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Search'
    WHEN REGEXP_CONTAINS(session_source,'badoo|facebook|Facebook|fb|instagram|ig|linkedin|pinterest|tiktok|twitter|social|meta') AND REGEXP_CONTAINS(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|paid.*)$') THEN 'Paid Social'
    WHEN REGEXP_CONTAINS(session_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
    AND REGEXP_CONTAINS(session_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Video'
    WHEN session_medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
    WHEN REGEXP_CONTAINS(session_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
    OR REGEXP_CONTAINS(session_campaign, '^(.*(([^a-df-z]|^)shop|shopping).*)$') THEN 'Organic Shopping'
    WHEN REGEXP_CONTAINS(session_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|social|meta') OR session_medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') THEN 'Organic Social'
    WHEN REGEXP_CONTAINS(session_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
    OR REGEXP_CONTAINS(session_medium,'^(.*video.*)$') THEN 'Organic Video'
    WHEN REGEXP_CONTAINS(session_source,'chatgpt|edgeservices|gemini|copilot|perplexity|claude|deepseek|grok') THEN 'Organic Search AI'
    WHEN REGEXP_CONTAINS(session_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') OR session_medium = 'organic' THEN 'Organic Search'
    WHEN REGEXP_CONTAINS(session_source,'email|e-mail|e_mail|e mail|care-mail')
    OR REGEXP_CONTAINS(session_medium,'email|e-mail|e_mail|e mail|caremail') THEN 'Email'
    WHEN session_medium = 'affiliate' THEN 'Affiliates'
    WHEN REGEXP_CONTAINS(session_source,'gaspedaal')
    OR session_medium = 'referral' THEN 'Referral'
    WHEN session_medium = 'audio' THEN 'Audio'
    WHEN session_medium = 'sms' THEN 'SMS'
    WHEN session_medium LIKE '%push' OR REGEXP_CONTAINS(session_medium,'mobile|notification') THEN 'Mobile Push Notifications'
    WHEN session_source = 'offline' THEN 'DM'
    ELSE NULL
    END AS custom_default_channel_group,
    NULLIF(concat(session_source,' / ', session_medium), ' / ') AS session_source_medium,

    FROM(
        SELECT
        * EXCEPT(session_source, session_medium, session_campaign),
        IFNULL(NULLIF(session_source, '(not set)'), first_user_source) AS session_source,
    IFNULL(NULLIF(session_medium, '(not set)'), first_user_medium) AS session_medium,
    IFNULL(NULLIF(session_campaign, '(not set)'), first_user_campaign_name) AS session_campaign,
    CONCAT(IFNULL(event_buy_brand, ""), IFNULL(item_brand, ""), IFNULL(event_name, ""), IFNULL(event_page_title, ""), IFNULL(event_page_location, ""), IFNULL(event_page_referrer, "")) as event_merk_concat,
    CONCAT(IFNULL(event_buy_brand, ""), IFNULL(session_google_ads_ad_group_name, ""), IFNULL(session_campaign, ""), IFNULL(session_landingpage_title, ""), IFNULL(session_landingpage_location, ""), IFNULL(session_term, ""), IFNULL(session_content, "")) as session_merk_concat,

    FROM(
        SELECT
    events.account,
    events.unique_event_id,
    PARSE_DATE('%Y%m%d',event_date) as event_date,
    event_timestamp,
    events.event_name,
    events.user_pseudo_id,
    sessie_assignment.user_pseudo_id AS sessie_user_pseudo_id,
    event_ga_session_id,
    events.privacy_analytics_storage,
    events.privacy_ads_storage,
    IF(events.user_pseudo_id IS NULL AND CAST(event_ga_session_id AS STRING) IS NULL AND events.event_name = 'session_start', unique_event_id, NULL) as privacy_session_id,
    event_bundle_sequence_id,
    event_page_referrer,
    event_page_location,
    event_page_title,
    event_entrances,
    event_engagement_time_msec,
    event_session_engaged,
    IFNULL(session_source,
        IF(events.collected_traffic_source_gclid <> "", events.cross_channel_campaign_last_click_source,
            IFNULL(events.collected_traffic_source_manual_source, events.cross_channel_campaign_last_click_source))
    ) AS session_source,
    IFNULL(session_medium,
        IF(events.collected_traffic_source_gclid <> "", events.cross_channel_campaign_last_click_medium,
            IFNULL(events.collected_traffic_source_manual_medium, events.cross_channel_campaign_last_click_medium))
    ) AS session_medium,
    IFNULL(session_campaign,
        IF(events.collected_traffic_source_gclid <> "", events.cross_channel_campaign_last_click_campaign_name,
            IFNULL(events.collected_traffic_source_manual_campaign_name, events.cross_channel_campaign_last_click_campaign_name))
    ) AS session_campaign,
    IFNULL(session_default_channel_group, events.cross_channel_campaign_last_click_default_channel_group) AS session_default_channel_group,
    sessie_assignment.ga_session_number,
    session_landingpage_referrer,
    session_landingpage_location,
    session_landingpage_title,
    submission_id_otm,
    conversion_value_ga4,
    IFNULL(session_engaged, IFNULL(events.event_engaged_session_event, 0)) AS session_engaged,
    IFNULL(sessie_assignment.user_first_touch_timestamp, events.user_first_touch_timestamp) AS user_first_touch_timestamp,
    IFNULL(sessie_assignment.is_active_user, events.is_active_user) as is_active_user,
    IFNULL(first_user_source, traffic_source_source) AS first_user_source,
    IFNULL(first_user_medium, traffic_source_medium) AS first_user_medium,
    IFNULL(first_user_campaign_name, traffic_source_name) AS first_user_campaign_name,
    IFNULL(session_primary_channel_group, events.cross_channel_campaign_last_click_primary_channel_group) AS session_primary_channel_group,
    IFNULL(session_term, IFNULL(events.collected_traffic_source_manual_term, events.manual_campaign_last_click_term)) AS session_term,
    IFNULL(session_content, IFNULL(events.collected_traffic_source_manual_content, events.manual_campaign_last_click_content)) AS session_content,
    IFNULL(session_campaign_id,  events.cross_channel_campaign_last_click_campaign_id) AS session_campaign_id,
    IFNULL(session_google_ads_customer_id, events.google_ads_campaign_last_click_customer_id) AS session_google_ads_customer_id,
    IFNULL(session_google_ads_account_name, google_ads_campaign_last_click_account_name) AS session_google_ads_account_name,
    IFNULL(session_google_ads_ad_group_id, google_ads_campaign_last_click_ad_group_id) AS session_google_ads_ad_group_id,
    IFNULL(session_google_ads_ad_group_name, google_ads_campaign_last_click_ad_group_name) AS session_google_ads_ad_group_name,
    IFNULL(session_device_category, device_category) AS session_device_category,
    IFNULL(session_device_brand, device_mobile_brand_name) AS session_device_brand,
    IFNULL(session_device_model, IFNULL(device_mobile_marketing_name, device_mobile_model_name)) AS session_device_model,
    IFNULL(session_device_operating_system, device_operating_system) AS session_device_operating_system,
    IFNULL(session_device_is_limited_ad_tracking, device_is_limited_ad_tracking) AS session_device_is_limited_ad_tracking,
    IFNULL(session_device_browser, device_webinfo_browser) AS session_device_browser,
    IFNULL(session_geo_city, geo_city) AS session_geo_city,
    IFNULL(session_geo_country, geo_country) AS session_geo_country,
    IFNULL(session_geo_region, geo_region) AS session_geo_region,
    IFNULL(session_geo_continent, geo_continent) AS session_geo_continent,
    IFNULL(events.event_buy_status, sessie_assignment.event_buy_status) AS event_buy_status,
    IFNULL(events.event_buy_model, sessie_assignment.event_buy_model) AS event_buy_model,
    IFNULL(events.event_buy_plaats, sessie_assignment.event_buy_plaats) AS event_buy_plaats,
    IFNULL(events.event_buy_brand, sessie_assignment.event_buy_brand) AS event_buy_brand,
    IFNULL(events.event_trade_in_model, sessie_assignment.event_trade_in_model) AS event_trade_in_model,
    IFNULL(events.event_trade_in_brand, sessie_assignment.event_trade_in_brand) AS event_trade_in_brand,
    IFNULL(events.event_formfields_id, sessie_assignment.event_formfields_id) AS event_formfields_id,
    IFNULL(events.event_formfields_merk, sessie_assignment.event_formfields_merk) AS event_formfields_merk,
    IFNULL(events.event_formfields_model, sessie_assignment.event_formfields_model) AS event_formfields_model,
    IFNULL(events.event_formfields_soort, sessie_assignment.event_formfields_soort) AS event_formfields_soort,
    IFNULL(events.event_formfields_titel, sessie_assignment.event_formfields_titel) AS event_formfields_titel,
    IFNULL(events.event_formfields_vestiging, sessie_assignment.event_formfields_vestiging) AS event_formfields_vestiging,
    IFNULL(events.event_buy_bouwjaar, sessie_assignment.event_buy_bouwjaar) AS event_buy_bouwjaar,
    IFNULL(events.event_buy_kleur, sessie_assignment.event_buy_kleur) AS event_buy_kleur,
    IFNULL(events.event_buy_type, sessie_assignment.event_buy_type) AS event_buy_type,
    IFNULL(events.event_buy_item_id, sessie_assignment.event_buy_item_id) AS event_buy_item_id,
    IFNULL(events.event_trade_in_bouwjaar, sessie_assignment.event_trade_in_bouwjaar) AS event_trade_in_bouwjaar,
    IFNULL(events.event_trade_in_brandstof, sessie_assignment.event_trade_in_brandstof) AS event_trade_in_brandstof,
    items.* EXCEPT(unique_event_id),
    ${ifSource("stg_ga4_dealerevents", `IFNULL(events.dealer_sessie, sessie_assignment.dealer_sessie) AS dealer_sessie,
    IFNULL(events.dealer_event, sessie_assignment.dealer_event) AS dealer_event,
    events.email AS email`)}


    FROM ${ref("df_rawdata_views","ga4_events")} events

    LEFT JOIN ${ref("df_staging_views","stg_ga4_sessie_assignment")} sessie_assignment
    ON events.user_pseudo_id = sessie_assignment.user_pseudo_id 
    AND events.event_ga_session_id = sessie_assignment.ga_session_id
    AND events.account = sessie_assignment.account

    LEFT JOIN ${ref("df_rawdata_views","ga4_items")} items
    ON events.unique_event_id = items.unique_event_id 
    AND events.account = sessie_assignment.account
    
    )))

`
let refs = getRefs()
module.exports = {query, refs}
