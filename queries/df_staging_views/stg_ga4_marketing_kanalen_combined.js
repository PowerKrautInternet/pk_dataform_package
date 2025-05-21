/*config*/
const {join, ref} = require("../../sources");
let query = `

SELECT 
    ga4.* EXCEPT(bron, kanaal, session_campaign, event_date, session_campaign_id, session_google_ads_ad_group_id, session_google_ads_ad_group_name, mapping_thema, ac_name, ac_workflow_edm, ac_campaign),
    marketing_kanalen.* EXCEPT(bron, campaign_name, record_date, campaign_id, ad_group_id, ad_group_name, merk),
    IFNULL(IFNULL(IFNULL(IFNULL(ga4.bron, marketing_kanalen.bron), searchconsole.bron), syntec.bron), ac.bron) as bron,
    IFNULL(IFNULL(IFNULL(IFNULL(ga4.kanaal, marketing_kanalen.bron), searchconsole.bron), syntec.kanaal), ac.kanaal) as kanaal,
    IFNULL(IFNULL(ga4.session_campaign, marketing_kanalen.campaign_name), syntec.onderwerp) as campaign_name,
    IFNULL(IFNULL(IFNULL(IFNULL(ga4.event_date, marketing_kanalen.record_date), searchconsole.data_date), syntec.record_date), ac.record_datum) as record_date,
    IFNULL(ga4.session_campaign_id, marketing_kanalen.campaign_id) as campaign_id,
    IFNULL(ga4.session_google_ads_ad_group_id, marketing_kanalen.ad_group_id) as ad_group_id,
    IFNULL(ga4.session_google_ads_ad_group_name, marketing_kanalen.ad_group_name) as ad_group_name,
    IFNULL(ga4.session_landingpage_location, searchconsole.url) as landingpage_location,
    IFNULL(ga4.session_term, searchconsole.query) as term,
    IFNULL(ga4.session_device_category, LOWER(searchconsole.device)) as device_category,
    IFNULL(ga4.session_geo_country, searchconsole.country) as geo_country,
    IFNULL(IFNULL(ga4.merk_event, marketing_kanalen.merk), syntec.merk) as merk,
    searchconsole.impressions as gsc_impressions,
    searchconsole.clicks as gsc_clicks,
    searchconsole.sum_position as gsc_sum_position,
    searchconsole.average_position as gsc_average_position,
    syntec.* EXCEPT(bron, kanaal, onderwerp, record_date, merk),
    IFNULL(mapping_thema, flow_thema) AS ac_flow_thema,
    IFNULL(ac.ac_name, ga4.ac_name) AS ac_name,
    IFNULL(ac.campaign_name, ga4.ac_campaign) AS ac_campaign,
    ac_subject_name,
    ac.contacts_entered AS ac_contacts_entered,
    ac.flow_campaigns AS ac_flow_campaigns,
    ac.total_sends AS ac_total_sends,
    ac.unique_opens AS ac_unique_opens,
    ac.total_clicks AS ac_total_clicks,
    ac.unsubscribes AS ac_unsubscribes,
    ac.total_bounces AS ac_total_bounces,
    ac.open_rate AS ac_open_rate,
    ac.click_rate AS ac_click_rate,
    ac.unsubscribe_rate AS ac_unsubscribe_rate,
    ac.forward_rate AS ac_forward_rate,
    ac.bounce_rates AS ac_bounce_rates,
    ac.click_to_open_ratio AS ac_click_to_open_ratio,
    ac.workflow_status AS ac_workflow_status,
    IFNULL(ac_workflow_edm, ac_bron) AS ac_bron,
    ac.aantal_contacts AS ac_aantal_contacts

FROM (SELECT 'GA4' as bron, * FROM ${ref("df_staging_views", "stg_ga4_mappings_targets")}) ga4
    
${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingkanalen_combined", "AS marketing_kanalen ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingdashboard_searchconsole", "AS searchconsole ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_syntec_leads_orders_combined", "AS syntec ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_Views", "stg_activecampaign_ga4_sheets", "AS ac ON 1=0")}

`
let refs = pk.getRefs()
module.exports = {query, refs}