/*config*/
const {join, ref, getRefs, ifSource, ifNull, orSource} = require("../../sources");
let query = `
SELECT
* ${orSource(["stg_handmatige_uitgaves_pivot", "gs_kostenlefmapping"], "EXCEPT(campagnegroep), IFNULL(campagnegroep, uitgave_categorie) AS campagnegroep")}
FROM(
SELECT * ${ifSource("gs_campagnegroepen", "EXCEPT(campagnegroep, campagne, account), IFNULL(ga4_ads.campagnegroep, groep.campagne) AS campagnegroep, ga4_ads.account AS account")}

FROM(
SELECT
    ga4.* EXCEPT(bron, kanaal, session_campaign, event_date, session_campaign_id, session_google_ads_ad_group_id, session_google_ads_ad_group_name, event_name, event_page_location,
    session_landingpage_title,
    session_geo_city,
    session_source_medium,
    user_pseudo_id,
    submission_id_otm,
    account ${ifSource("stg_hubspot_workflowstats",", hs_workflow_name, edm_name")}
    ),
    ${ifSource("stg_marketingkanalen_combined", "marketing_kanalen.* EXCEPT(bron, campaign_name, record_date, campaign_id, ad_group_id, ad_group_name, merk, account")}
    ${ifSource("stg_handmatige_uitgaves_pivot", ", uitgave_categorie")}
    ${ifSource("stg_marketingkanalen_combined", "),")}
    ${ifSource("stg_handmatige_uitgaves_pivot", "marketing_kanalen.uitgave_categorie AS handmatige_uitgave_categorie,")}
    ${ifNull([
        "ga4.bron",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.bron"),
        ifSource("stg_lef_leads_agg", "lef.bron"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.bron"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.bron"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.bron"),
        ifSource("stg_hubspot_workflowstats", "hs_bron"),
        ifSource("stg_otm_aggregated", "otm.bron"),
    ])} as bron,
    ${ifNull([
        "ga4.account",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.account"),
        ifSource("stg_lef_leads_agg", "lef.account"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.account"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.account"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.account")
    ])} as account,
    ${ifNull([
        "ga4.kanaal",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.bron"),
        ifSource("stg_lef_leads_agg", "lef.kanaal"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.bron"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.kanaal"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.kanaal"),
        ifSource("stg_hubspot_workflowstats","hs.kanaal"),
        ifSource("stg_otm_aggregated","kanaal_otm"),
    ])} as kanaal,
    ${ifNull([
        "ga4.session_campaign",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.campaign_name"),
        ifSource("stg_lef_leads_agg", "lef.session_campaign"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.onderwerp"),
        ifSource("stg_hubspot_workflowstats", "hs.session_campaign"),
        ifSource("stg_otm_aggregated", "session_campaign_otm"),
    ])} as campaign_name,
    ${ifNull([
        "ga4.event_date",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.record_date"),
        ifSource("stg_lef_leads_agg", "CAST(lef.aangemaaktDatum AS DATE)"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.data_date"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.record_date"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.record_datum"),
        ifSource("stg_hubspot_workflowstats", "hs_date"),
        ifSource("stg_otm_aggregated", "created_at_date_otm")
    ])} as record_date,
    ${ifNull([
        "ga4.session_campaign_id",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.campaign_id")
    ], "as campaign_id,")}
    ${ifNull(["ga4.session_google_ads_ad_group_id",     ifSource("stg_marketingkanalen_combined",           "marketing_kanalen.ad_group_id")],      "as ad_group_id,")}
    ${ifNull(["ga4.session_google_ads_ad_group_name",   ifSource("stg_marketingkanalen_combined",           "marketing_kanalen.ad_group_name")],    " as ad_group_name,")}
    ${ifNull(["ga4.session_landingpage_location",       
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.url"),
        ifSource("stg_lef_leads_agg","lef.session_landingpage_location")     
    ], "as landingpage_location,")}
    ${ifNull(["ga4.session_term",                       ifSource("stg_marketingdashboard_searchconsole",    "searchconsole.query")],                "as term,")}
    ${ifNull(["ga4.session_device_category",            
        ifSource("stg_marketingdashboard_searchconsole", "LOWER(searchconsole.device)"),
        ifSource("stg_lef_leads_agg","lef.session_device_category")
    ], "as device_category,")}
    ${ifNull(["ga4.session_geo_country",                ifSource("stg_marketingdashboard_searchconsole",    "searchconsole.country")],              "as geo_country,")}
    ${ifNull([
        ifSource("gs_merken", "ga4.merk_event"),                         
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.merk"),
        ifSource("stg_lef_leads_agg", "lef.merk_session"),
        ifSource("stg_syntec_leads_orders_combined", "syntec.merk"),
        "'Overig'"
    ], "as merk,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.impressions as gsc_impressions,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.clicks as gsc_clicks,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.sum_position as gsc_sum_position,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.average_position as gsc_average_position,")}
    ${ifSource("stg_syntec_leads_orders_combined", "syntec.* EXCEPT(bron, kanaal, onderwerp, record_date, merk, account, model),")}
    ${ifSource("stg_syntec_leads_orders_combined", "syntec.model AS syntec_model,")}
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
    
    ${ifNull(["ga4.event_name", 
        ifSource("stg_lef_leads_agg","lef.event_name"
    )], "AS event_name,")}
    
    ${ifNull(["ga4.event_page_location",
        ifSource("stg_lef_leads_agg","lef.event_page_location"
    )], "AS event_page_location,")}
    
    ${ifNull(["ga4.session_landingpage_title",
        ifSource("stg_lef_leads_agg","lef.session_landingpage_title")
    ], "AS session_landingpage_title,")}
    
    ${ifNull(["ga4.session_geo_city",
        ifSource("stg_lef_leads_agg","lef.session_geo_city")
    ], "AS session_geo_city,")}
    
    ${ifNull(["ga4.session_source_medium",
        ifSource("stg_lef_leads_agg","lef.session_source_medium"),
        ifSource("stg_hubspot_workflowstats", "hs.session_source_medium"),
        ifSource("stg_otm_aggregated", "session_source_medium_otm"),
    ], "AS session_source_medium,")}

    ${ifNull(["ga4.user_pseudo_id",
        ifSource("stg_lef_leads_agg","lef.google_clientid")
    ], "AS user_pseudo_id,")}

    ${ifSource("stg_lef_leads_agg","lef.LEFleadID AS lef_lead_id,")}
    ${ifSource("stg_lef_leads_agg","lef.aangemaaktDatum AS lef_aangemaakt_datum,")}
    ${ifSource("stg_lef_leads_agg","lef.afgerondDatum AS lef_afgerond_datum,")}
    ${ifSource("stg_lef_leads_agg","lef.lead_bron AS lef_lead_bron,")}
    ${ifSource("stg_lef_leads_agg","lef.systeem AS lef_systeem,")}
    ${ifSource("stg_lef_leads_agg","lef.kwalificatie AS lef_kwalificatie,")}
    ${ifSource("stg_lef_leads_agg","lef.leadType AS lef_lead_type,")}
    ${ifSource("stg_lef_leads_agg","lef.initiatief AS lef_initiatief,")}
    ${ifSource("stg_lef_leads_agg","lef.soortLead AS lef_soort_lead,")}
    ${ifSource("stg_lef_leads_agg","lef.leadOmschrijving AS lef_lead_omschrijving,")}
    ${ifSource("stg_lef_leads_agg","lef.vestiging AS lef_vestiging,")}
    ${ifSource("stg_lef_leads_agg","lef.medewerker AS lef_medewerker,")}
    ${ifSource("stg_lef_leads_agg","lef.resultaat AS lef_resultaat,")}
    ${ifSource("stg_lef_leads_agg","lef.afsluitreden AS lef_afsluitreden,")}
    ${ifSource("stg_lef_leads_agg","lef.heeftOfferte AS lef_heeft_offerte,")}
    ${ifSource("stg_lef_leads_agg","lef.heeftOrder AS lef_heeft_order,")}
    ${ifSource("stg_lef_leads_agg","lef.ordernummer AS lef_ordernummer,")}
    ${ifSource("stg_lef_leads_agg","lef.dealernummer AS lef_order_dealernummer,")}
    ${ifSource("stg_lef_leads_agg","lef.gewenstModel AS lef_model,")}
    ${ifSource("stg_lef_leads_agg","lef.gewenstAutoSoort AS lef_autosoort,")}
    ${ifSource("stg_lef_leads_agg","lef.gewenstBrandstof AS lef_brandstof,")}
    ${ifSource("stg_lef_leads_agg","lef.gewenstBouwjaar AS lef_bouwjaar,")}
    ${ifSource("stg_sam_offertes","offerte_SALESTRAJECT_TRAJECTID, offerte_SALESTRAJECT_AFGERONDDATUM, offerte_SALESTRAJECT_CREATIEDATUM, offerte_OFFERTESTATUS_OMSCHRIJVING, offerte_OFFERTE_TOTAALBEDRAG, offerte_HERKOMST_OMSCHRIJVING, offerte_OFFERTE_OFFERTEID, getekende_offertes, offerte_SALESTRAJECT_TRAJECTSTATUSID, offerte_OFFERTEVTR_BRUTOMARGEBEDRAG, offerte_MERK_OMSCHRIJVING, offerte_AFLEVERINGMODEL_OMSCHRIJVING, offerte_DEALER_NAAM, offerte_VERKOPER_NAAM,")}
    ${ifSource("stg_hubspot_workflowstats", "hs.* EXCEPT(hs_date, hs_bron, session_campaign, session_source_medium, kanaal, hs_workflow_name, edm_name),")}
    ${ifNull([ifSource("stg_hubspot_workflowstats", "hs.hs_workflow_name"), ifSource("stg_hubspot_workflowstats", "ga4.hs_workflow_name")], "AS hs_workflow_name,")}
    ${ifNull([ifSource("stg_hubspot_workflowstats", "hs.edm_name"), ifSource("stg_hubspot_workflowstats", "ga4.edm_name")], "AS edm_name,")}
    ${ifNull([ifSource("stg_handmatige_uitgaves_pivot", "marketing_kanalen.uitgave_categorie"), ifSource("gs_kostenlefmapping","lef.uitgave_categorie")], "AS uitgave_categorie,")}
    ${ifNull([ifSource("stg_handmatige_uitgaves_pivot", "marketing_kanalen.bron"), ifSource("gs_kostenlefmapping","lef.kanaal")], "AS uitgave_bron,")}
    ${ifNull(["CAST(ga4.submission_id_otm AS STRING)", ifSource("stg_otm_aggregated", "otm.submission_id_otm")], "AS submission_id_otm,")}
    ${ifSource("stg_otm_aggregated", "otm.* EXCEPT(created_at_date_otm, session_campaign_otm, session_source_medium_otm, kanaal_otm, bron, submission_id_otm)")}

FROM (SELECT 'GA4' as bron, * FROM ${ref("df_staging_views", "stg_ga4_mappings_targets")}) ga4
    
${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingkanalen_combined", "AS marketing_kanalen ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingdashboard_searchconsole", "AS searchconsole ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_syntec_leads_orders_combined", "AS syntec ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_activecampaign_ga4_sheets", "AS ac ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_lef_leads_agg", "AS lef ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_hubspot_workflowstats", "AS hs ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_otm_aggregated", "AS otm ON 1=0")}
) ga4_ads
${join("LEFT JOIN", "df_googlesheets_tables", "gs_campagnegroepen", `AS groep ON LOWER(campaign_name) LIKE LOWER(CONCAT(\"%\", groep.campagnegroep, \"%\")) ${ifSource("stg_lef_leads_agg", `OR LOWER(lef_kwalificatie) LIKE LOWER(CONCAT(\"%\", groep.campagnegroep, \"%\"))`)}
)

`
let refs = getRefs()
module.exports = {query, refs}
