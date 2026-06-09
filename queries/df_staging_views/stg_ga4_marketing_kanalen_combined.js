/*config*/
const {join, ref, getRefs, ifSource, ifNull, orSource} = require("../../sources");
let query = `

-- CTE 1: alle bronnen samenvoegen via FULL OUTER JOIN ON 1=0 (union-patroon)
WITH ga4_base AS (
  SELECT 'GA4' AS bron, * FROM ${ref("df_staging_views", "stg_ga4_mappings_targets")}
),

ga4_ads AS (
  SELECT
    ga4.* EXCEPT(account, bron, kanaal, session_campaign, event_date, session_campaign_id, session_google_ads_ad_group_id, session_google_ads_ad_group_name, event_name, event_page_location,
    session_landingpage_title,
    session_geo_city,
    session_source_medium,
    user_pseudo_id,
    submission_id_otm
    ${ifSource("gs_activecampaign_ga4_mapping",", ac_name")}
    ${ifSource("stg_hubspot_workflowstats",", hs_workflow_name, edm_name")}
    ${ifSource("gs_activecampaign_ga4_mapping",", ac_campaign")}
    ),
    ${ifSource("stg_marketingkanalen_combined", "marketing_kanalen.* EXCEPT(bron, campaign_name, record_date, campaign_id, ad_group_id, ad_group_name, merk, model, account, advertiser_name")}
    ${ifSource("stg_handmatige_uitgaves_pivot", ", uitgave_categorie")}
    ${ifSource("stg_marketingkanalen_combined", "),")}
    ${ifSource("stg_marketingkanalen_combined", "advertiser_name AS account_name,")}
    ${ifSource("stg_handmatige_uitgaves_pivot", "marketing_kanalen.uitgave_categorie AS handmatige_uitgave_categorie,")}

    -- Gemeenschappelijke dimensies: bron, account, kanaal, campagne, datum
    ${ifNull([
        "ga4.bron",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.bron"),
        orSource(["stg_lef_leads_agg", "stg_syntec_leads_orders_combined"], "crm.bron"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.bron"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.bron"),
        ifSource("stg_hubspot_workflowstats", "hs_bron"),
        ifSource("stg_otm_aggregated", "otm.bron"),
    ])} AS bron,
    ${ifNull([
        "ga4.account",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.account"),
        orSource(["stg_lef_leads_agg", "stg_syntec_leads_orders_combined"], "crm.account"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.account"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.account"),
        ifSource("stg_otm_aggregated", "otm.account")
    ])} AS account,
    ${ifNull([
        "ga4.kanaal",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.bron"),
        orSource(["stg_lef_leads_agg", "stg_syntec_leads_orders_combined"], "crm.kanaal"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.bron"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.kanaal"),
        ifSource("stg_hubspot_workflowstats","hs.kanaal"),
        ifSource("stg_otm_aggregated","kanaal_otm"),
    ])} AS kanaal,
    ${ifNull([
        "ga4.session_campaign",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.campaign_name"),
        orSource(["stg_lef_leads_agg", "stg_syntec_leads_orders_combined"], "crm.session_campaign"),
        ifSource("stg_hubspot_workflowstats", "hs.session_campaign"),
        ifSource("stg_otm_aggregated", "session_campaign_otm"),
    ])} AS campaign_name,
    ${ifNull([
        "ga4.event_date",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.record_date"),
        orSource(["stg_lef_leads_agg", "stg_syntec_leads_orders_combined"], "crm.record_date"),
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.data_date"),
        ifSource("stg_activecampaign_ga4_sheets", "ac.record_datum"),
        ifSource("stg_hubspot_workflowstats", "hs_date"),
        ifSource("stg_otm_aggregated", "created_at_date_otm")
    ])} AS record_date,
    ${ifNull([
        "ga4.session_campaign_id",
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.campaign_id")
    ], "AS campaign_id,")}
    ${ifNull(["ga4.session_google_ads_ad_group_id", ifSource("stg_marketingkanalen_combined", "marketing_kanalen.ad_group_id")], "AS ad_group_id,")}
    ${ifNull([`IF(ga4.kanaal IN ('META', 'Microsoft Ads', 'Google Ads'), ga4.session_content, ga4.session_google_ads_ad_group_name)`, ifSource("stg_marketingkanalen_combined", "marketing_kanalen.ad_group_name")], "AS ad_group_name,")}

    -- Pagina & device dimensies
    ${ifNull(["ga4.session_landingpage_location",
        ifSource("stg_marketingdashboard_searchconsole", "searchconsole.url"),
        ifSource("stg_lef_leads_agg","crm.session_landingpage_location")
    ], "AS landingpage_location,")}
    ${ifNull(["ga4.session_term", ifSource("stg_marketingdashboard_searchconsole", "searchconsole.query")], "AS term,")}
    ${ifNull(["ga4.session_device_category",
        ifSource("stg_marketingdashboard_searchconsole", "LOWER(searchconsole.device)"),
        ifSource("stg_lef_leads_agg","crm.session_device_category")
    ], "AS device_category,")}
    ${ifNull(["ga4.session_geo_country", ifSource("stg_marketingdashboard_searchconsole", "searchconsole.country")], "AS geo_country,")}

    -- Merk
    ${ifNull([
        ifSource("gs_merken", "ga4.merk_event"),
        ifSource("stg_marketingkanalen_combined", "marketing_kanalen.merk"),
        orSource(["stg_lef_leads_agg", "stg_syntec_leads_orders_combined"], "crm.merk"),
        "'Overig'"
    ], "AS merk,")}

    -- Model (gecombineerde lookup tegen gs_modellen op basis van GA4-signalen + Ads-side text)
    ${ifSource('gs_modellen', `${ref("lookup_table_sql")}(
        TRIM(CONCAT(
            IFNULL(ga4.vehicle_nameplate, ''), ' ',
            IFNULL(ga4.event_buy_model, ''), ' ',
            IFNULL(ga4.session_content, ''), ' ',
            IFNULL(ga4.session_campaign, ''), ' ',
            ${ifSource("stg_marketingkanalen_combined", "IFNULL(marketing_kanalen.ads_merk_concat, ''), ' ', IFNULL(marketing_kanalen.model, ''),")}
            ''
        )),
        lookup_modellen.haystack
    )`)} AS model,

    -- Google Search Console metrics
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.impressions AS gsc_impressions,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.clicks AS gsc_clicks,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.sum_position AS gsc_sum_position,")}
    ${ifSource("stg_marketingdashboard_searchconsole", "searchconsole.average_position AS gsc_average_position,")}

    -- Syntec velden
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_lead_id,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_ordersoort,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_onderwerp,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_vestiging,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_model,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_kanaal,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_kanaal_groep,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_kenteken,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_aangelegd,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_datum_gesloten,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_sluitreden,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_lead_order_id,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_klantkoppeling,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_verkoper,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_laatste_opmerking,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_contactmomenten,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_min_voor_geopend,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_order_id,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_order_status,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_date_delivery,")}
    ${ifSource("stg_syntec_leads_orders_combined", "crm.syntec_customergroup,")}

    -- ActiveCampaign velden
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
    ${ifSource("stg_activecampaign_ga4_sheets", "ac.aantal_contacts AS ac_aantal_contacts,")}

    -- GA4 event velden met fallback op CRM
    ${ifNull(["ga4.event_name",
        ifSource("stg_lef_leads_agg","crm.event_name")
    ], "AS event_name,")}
    ${ifNull(["ga4.event_page_location",
        ifSource("stg_lef_leads_agg","crm.event_page_location")
    ], "AS event_page_location,")}
    ${ifNull(["ga4.session_landingpage_title",
        ifSource("stg_lef_leads_agg","crm.session_landingpage_title")
    ], "AS session_landingpage_title,")}
    ${ifNull(["ga4.session_geo_city",
        ifSource("stg_lef_leads_agg","crm.session_geo_city")
    ], "AS session_geo_city,")}
    ${ifNull(["ga4.session_source_medium",
        ifSource("stg_lef_leads_agg","crm.session_source_medium"),
        ifSource("stg_hubspot_workflowstats", "hs.session_source_medium"),
        ifSource("stg_otm_aggregated", "session_source_medium_otm"),
    ], "AS session_source_medium,")}
    ${ifNull(["ga4.user_pseudo_id",
        ifSource("stg_lef_leads_agg","crm.user_pseudo_id")
    ], "AS user_pseudo_id,")}

    -- LEF CRM velden
    ${ifSource("stg_lef_leads_agg","crm.lef_lead_id,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_aangemaakt_datum,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_afgerond_datum,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_lead_bron,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_systeem,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_kwalificatie,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_lead_type,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_initiatief,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_soort_lead,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_lead_omschrijving,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_vestiging,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_medewerker,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_resultaat,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_afsluitreden,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_heeft_offerte,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_heeft_order,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_ordernummer,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_dealernummer,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_gewenst_model,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_gewenst_merk,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_gewenst_autosoort,")}
    ${ifSource("stg_lef_leads_agg","crm.gewenst_model,")}
    ${ifSource("stg_lef_leads_agg","crm.gewenst_autosoort,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_brandstof,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_bouwjaar,")}
    ${ifSource("stg_lef_leadopvolging_avg","crm.mean_doorlooptijd_hours,")}
    ${ifSource("stg_lef_leadopvolging_avg","crm.std_doorlooptijd_hours,")}
    ${ifSource("stg_lef_leadopvolging_avg","crm.mean_deals,")}
    ${ifSource("stg_lef_leadopvolging_avg","crm.std_deals,")}
    ${ifSource("stg_lef_leads_avg","crm.mean_leads,")}
    ${ifSource("stg_lef_leads_avg","crm.std_leads,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_eerste_contactpoging,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_laatste_status_startgesprek,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_deadline_gehaald,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_deadline_gehaald_importeur,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_eerste_deadline,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_eerste_deadline_importeur,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_doorlooptijd_tot_ingezien,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_doorlooptijd_tot_eerste_contactpoging,")}
    ${ifSource("stg_lef_leads_agg","crm.lef_ingezien_datum,")}

    -- SAM velden
    ${ifSource("stg_sam_offertes_orders","crm.sam_salestraject_id,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_salestraject_afgerond_datum,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_salestraject_creatie_datum,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_soort_auto,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_offerte_status,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_offerte_totaalbedrag,")}
    ${ifSource(["stg_sam_offertes_orders", "sam_herkomst"],"crm.sam_herkomst,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_offerte_id,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_getekende_offertes,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_salestraject_status_id,")}
    ${ifSource(["stg_sam_offertes_orders", "sam_offerte_vtr"],"crm.sam_brutomarge,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_merk,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_model,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_dealer_naam,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_verkoper_naam,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_leadtraject_id,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_leadtraject_eerste_kwalificatie,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_order_id,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_aflevertraject_aantal,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_afleveringstatus,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_soort_klantcategorie,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_soort_brandstof,")}
    ${ifSource("stg_sam_offertes_orders","crm.sam_order_datum,")}

    -- HubSpot velden
    ${ifSource("stg_hubspot_workflowstats", "hs.* EXCEPT(hs_date, hs_bron, session_campaign, session_source_medium, kanaal, hs_workflow_name, edm_name),")}
    ${ifNull([ifSource("stg_hubspot_workflowstats", "hs.hs_workflow_name"), ifSource("stg_hubspot_workflowstats", "ga4.hs_workflow_name")], "AS hs_workflow_name,")}
    ${ifNull([ifSource("stg_hubspot_workflowstats", "hs.edm_name"), ifSource("stg_hubspot_workflowstats", "ga4.edm_name")], "AS edm_name,")}

    -- Uitgave categorisatie
    ${ifNull([ifSource("stg_handmatige_uitgaves_pivot", "marketing_kanalen.uitgave_categorie"), orSource(["gs_kostenlefmapping", "gs_kostensyntecmapping"], "crm.uitgave_categorie")], "AS uitgave_categorie,")}
    ${ifNull([ifSource("stg_handmatige_uitgaves_pivot", "marketing_kanalen.bron"), orSource(["gs_kostenlefmapping", "gs_kostensyntecmapping"], "crm.uitgave_bron")], "AS uitgave_bron,")}

    -- OTM velden
    ${ifNull(["CAST(ga4.submission_id_otm AS STRING)", ifSource("stg_otm_aggregated", "otm.submission_id_otm")], "AS submission_id_otm,")}
    ${ifSource("stg_otm_aggregated", "otm.* EXCEPT(created_at_date_otm, session_campaign_otm, session_source_medium_otm, kanaal_otm, bron, submission_id_otm, account)")}

  FROM ga4_base ga4

  ${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingkanalen_combined", "AS marketing_kanalen ON 1=0")}
  ${join("FULL OUTER JOIN", "df_staging_views", "stg_marketingdashboard_searchconsole", "AS searchconsole ON 1=0")}
  ${join("FULL OUTER JOIN", "df_staging_views", "stg_crm_leads_combined", "AS crm ON 1=0")}
  ${join("FULL OUTER JOIN", "df_staging_views", "stg_activecampaign_ga4_sheets", "AS ac ON 1=0")}
  ${join("FULL OUTER JOIN", "df_staging_views", "stg_hubspot_workflowstats", "AS hs ON 1=0")}
  ${join("FULL OUTER JOIN", "df_staging_views", "stg_otm_aggregated", "AS otm ON 1=0")}
  ${ifSource('gs_modellen', `CROSS JOIN (SELECT INITCAP(TO_JSON_STRING(ARRAY(SELECT model FROM ${ref("df_googlesheets_tables", "gs_modellen", true)}))) AS haystack) lookup_modellen`)}
),

-- CTE 2: campagnegroep lookup + master_bu_concat opbouwen voor business unit classificatie
ga4_ads_campagne AS (
  SELECT
    ga4_ads.* ${ifSource("gs_campagnegroepen",`EXCEPT(campagnegroep),
    ${ifNull(["groep.campagnegroep", orSource(["gs_kostenlefmapping", "gs_kostensyntecmapping"], "uitgave_categorie")], "AS campagnegroep" )}`)},
    LOWER(ARRAY_TO_STRING([
        ${ifSource("stg_syntec_leads_orders_combined", "syntec_ordersoort,")}
        ${ifSource("stg_lef_leads_agg", "gewenst_autosoort,")}
        ${ifSource("stg_lef_leads_agg", "lef_lead_type,")}
        ${ifSource("stg_lef_leads_agg", "lef_soort_lead,")}
        ${ifSource("stg_lef_leads_agg", "lef_kwalificatie,")}
        ${ifSource("stg_marketingkanalen_combined", "ads_merk_concat,")}
        ${ifSource("stg_marketingdashboard_searchconsole", "term,")}
        ${ifSource("stg_marketingdashboard_searchconsole", "landingpage_location,")}
        event_buy_status,
        soort_conversie,
        event_merk_concat
      ], ' ')) AS master_bu_concat
  FROM ga4_ads
  ${join("LEFT JOIN", "df_googlesheets_tables", "gs_campagnegroepen", `AS groep ON LOWER(campaign_name) LIKE LOWER(CONCAT("%", groep.campagnegroep, "%")) ${ifSource("stg_lef_leads_agg", `OR LOWER(lef_kwalificatie) LIKE LOWER(CONCAT("%", groep.campagnegroep, "%"))`)}`)}
)

-- Eindresultaat: business unit classificatie op basis van master_bu_concat
SELECT
  * EXCEPT(master_bu_concat),
  CASE
    WHEN REGEXP_CONTAINS(lower(master_bu_concat), '(vacature|werkenbij|solliciteer|monteur|receptionist|stage|recruitment|job)')
      THEN 'HR'
    WHEN REGEXP_CONTAINS(lower(master_bu_concat), '(werkplaats|onderhoud|apk|beurt|reparatie|banden|wintercheck|zomercheck|airco|service|schade|onderdelen)')
      THEN 'Aftersales'
    WHEN REGEXP_CONTAINS(lower(master_bu_concat), '(zakelijk|fleet|lcv|bedrijfswagen|bestelwagen|operational|ondernemer|zzp|bijtelling)')
      THEN 'Zakelijk'
    WHEN REGEXP_CONTAINS(lower(master_bu_concat), '(occasion|gebruikt|voorraad|used|inruil|taxatie|waarde|inkoop|tweedehands)')
      THEN 'Occasions'
    WHEN REGEXP_CONTAINS(lower(master_bu_concat), '(nieuw|private|model|showroom|actie|offerte|configurator|proefrit|hybride|elektrisch|ev|phev|2024|2025|2026)')
      THEN 'Verkoop Nieuw'
    ELSE NULL
  END AS business_unit
FROM ga4_ads_campagne
`
let refs = getRefs()
module.exports = {query, refs}
