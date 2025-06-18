/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT
    ga4.* EXCEPT(bron, kanaal, session_campaign, event_date, session_campaign_id, session_google_ads_ad_group_id, session_google_ads_ad_group_name),
    ${ifSource("stg_marketingkanalen_combined", "marketing_kanalen.* EXCEPT(bron, campaign_name, record_date, campaign_id, ad_group_id, ad_group_name, merk),")}
    ${ifNull([
        "ga4.bron",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.bron"),
        ifSource("stg_lef_leads_agg", "lef.bron"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.bron"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.bron"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.bron")
    ])} as bron,
    ${ifNull([
        "ga4.kanaal",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.bron"),
        ifSource("stg_lef_leads_agg", "lef.kanaal"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.bron"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.kanaal"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.kanaal")
    ])} as kanaal,
    ${ifNull([
        "ga4.session_campaign",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.campaign_name"),
        ifSource("stg_lef_leads_agg", "lef.session_campaign"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.onderwerp")
    ])} as campaign_name,
    ${ifNull([
        "ga4.event_date",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.record_date"),
        ifSource("stg_lef_laeds_agg", "lef.aangemaaktDatum"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.data_date"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.record_date"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.record_datum")
    ])} as record_date,
    ${ifNull([
        "ga4.session_campaign_id",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.campaign_id")
    ], "as campaign_id,")}
    ${ifNull(["ga4.session_google_ads_ad_group_id",     ifSource("stg_marketingkanalen_combined",           "marketing_kanalen.ad_group_id")],      "as ad_group_id,")}
    ${ifNull(["ga4.session_google_ads_ad_group_name",   ifSource("stg_marketingkanalen_combined",           "marketing_kanalen.ad_group_name")],    " as ad_group_name,")}
    ${ifNull(["ga4.session_landingpage_location",       ifSource("stg_marketingdashboard_searchconsole",    "searchconsole.url")],                  " as landingpage_location,")}
    ${ifNull(["ga4.session_term",                       ifSource("stg_marketingdashboard_searchconsole",    "searchconsole.query")],                "as term,")}
    ${ifNull(["ga4.session_device_category",            ifSource("stg_marketingdashboard_searchconsole",    "LOWER(searchconsole.device)")],        "as device_category,")}
    ${ifNull(["ga4.session_geo_country",                ifSource("stg_marketingdashboard_searchconsole",    "searchconsole.country")],              "as geo_country,")}
    ${ifNull([
        "ga4.merk_event",                         
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.merk"),
        ifSource("stg_lef_leads_agg", "lef.merk"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.merk")
    ], "as merk,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.impressions as gsc_impressions,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.clicks as gsc_clicks,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.sum_position as gsc_sum_position,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.average_position as gsc_average_position,")}
    ${ifSource("stg_syntec_leads_orders_combined", "syntec.* EXCEPT(bron, kanaal, onderwerp, record_date, merk),")}
    ${ifNull([ifSource("gs_activecampaign_ga4_mapping","mapping_thema"), ifSource(["stg_activecampaign_ga4_sheets", "gs_activecampaign_ga4_mapping"], "flow_thema")], "AS ac_flow_thema,")} 
    ${ifNull([ifSource("stg_activecampaign_ga4_sheets", "ac.ac_name"), ifSource("gs_activecampaign_ga4_mapping","ga4.ac_name")], "AS ac_name,")} 
    ${ifNull([ifSource("stg_activecampaign_ga4_sheets", "ac.campaign_name"), ifSource("gs_activecampaign_ga4_mapping","ga4.ac_campaign")],"AS ac_campaign,")} 
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.ac_subject_name,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.contacts_entered AS ac_contacts_entered,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.flow_campaigns AS ac_flow_campaigns,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.total_sends AS ac_total_sends,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.unique_opens AS ac_unique_opens,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.total_clicks AS ac_total_clicks,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.unsubscribes AS ac_unsubscribes,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.total_bounces AS ac_total_bounces,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.open_rate AS ac_open_rate,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.click_rate AS ac_click_rate,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.unsubscribe_rate AS ac_unsubscribe_rate,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.forward_rate AS ac_forward_rate,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.bounce_rates AS ac_bounce_rates,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.click_to_open_ratio AS ac_click_to_open_ratio,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.workflow_status AS ac_workflow_status,")}
    ${ifNull([ifSource("gs_activecampaign_ga4_mapping","ac_workflow_edm"), ifSource("stg_activecampaign_ga4_sheets","ac_bron")], "AS ac_bron,")}
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.aantal_contacts AS ac_aantal_contacts")}
    ${ifSource("stg_lef_leads_agg", `lef.* EXCEPT(bron,
        event_name,
        event_page_location,
        session_primary_channel_group,
        session_landingpage_location,
        session_landingpage_title,
        session_device_category,
        session_geo_city,
        session_source_medium,
        kanaal,
        session_campaign,
        aangemaaktDatum,
        Merk),`)}
    
    ${ifNull(["ga4.event_name", 
        ifSource("stg_lef_leads_agg","lef.event_name"
    )], "AS event_name,")}
    
    ${ifNull(["ga4.event_page_location",
        ifSource("stg_lef_leads_agg","lef.event_page_location"
    )], "AS event_page_location,")}
    
    ${ifNull(["ga4.session_primary_channel_group",
        ifSource("stg_lef_leads_agg","lef.session_primary_channel_group")
    ],"AS session_primary_channel_group,")}
    
    ${ifNull(["ga4.session_landingpage_location",
        ifSource("stg_lef_leads_agg","lef.session_landingpage_location")
    ], "AS session_landingpage_location,")}
    
    ${ifNull(["ga4.session_landingpage_title",
        ifSource("stg_lef_leads_agg","lef.session_landingpage_title")
    ], "AS session_landingpage_title,")}
    
    ${ifNull(["ga4.session_device_category",
        ifSource("stg_lef_leads_agg","lef.session_device_category")
    ], "AS session_device_category,")}
    
    ${ifNull(["ga4.session_geo_city",
        ifSource("stg_lef_leads_agg","lef.session_geo_city")
    ], "AS session_geo_city,")}
    
    ${ifNull(["ga4.session_source_medium",
        ifSource("stg_lef_leads_agg","lef.session_source_medium")
    ], "AS session_source_medium,")}

FROM (SELECT 'GA4' as bron, * FROM ${ref("df_staging_views", "stg_ga4_mappings_targets")}) ga4
    
${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingkanalen_combined", "AS marketing_kanalen ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingdashboard_searchconsole", "AS searchconsole ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_syntec_leads_orders_combined", "AS syntec ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_activecampaign_ga4_sheets", "AS ac ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_lef_leads_agg", "AS lef ON 1=0")}

`
let refs = getRefs()
module.exports = {query, refs}