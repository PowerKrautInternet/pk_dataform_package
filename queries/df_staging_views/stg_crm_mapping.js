/*config*/
const {join, ref, getRefs, ifSource, ifNull, channelCase} = require("../../sources");
let query = `

-- CTE 1: Koppel LEF+SAM entiteiten aan GA4 sessies en dedupliceer naar eerste sessie per lead+traject
WITH lef_ga4 AS (
  SELECT
    lef.*,
    ${channelCase('lef.session_source', 'lef.session_medium')} AS sessie_conversie_bron,
    kanalen.event_timestamp AS ga4_event_timestamp
  FROM ${ref("df_staging_views", "stg_lef_sam_combined")} lef
  LEFT JOIN (
    SELECT * FROM ${ref("df_staging_tables", "stg_ga4_events_sessies")}
    WHERE event_name = "session_start"
  ) kanalen ON TRIM(lef.google_clientid) = TRIM(kanalen.user_pseudo_id) AND lef.account = kanalen.account
),

-- CTE 2: Dedupliceer naar één rij per lead+traject (vroegste GA4 sessie), voeg kostenmapping toe
lef_enriched AS (
  SELECT
    'LEF' AS bron,
    lef.* EXCEPT(lead_rank, kanaal, merk_session, lef_gewenst_model, lef_gewenst_autosoort),
    ${ifNull(['lef.sessie_conversie_bron', ifSource('gs_kostenlefmapping', 'mapping.uitgave_bron')])} AS kanaal,
    ${ifNull([ifSource('stg_sam_offertes_orders', 'lef.sam_merk'), 'lef.lef_gewenst_merk', 'lef.merk_session', ifSource('gs_kostenlefmapping', 'mapping.uitgave_merk')])} AS merk_session,
    ${ifNull([ifSource('stg_sam_offertes_orders', 'lef.sam_model'), 'lef.lef_gewenst_model'])} AS lef_gewenst_model,
    ${ifNull([ifSource('stg_sam_offertes_orders', 'lef.sam_soort_auto'), 'lef.lef_gewenst_autosoort'])} AS lef_autosoort,
    lef.medewerker AS lef_medewerker,
    lef.vestiging AS lef_vestiging,
    ${ifSource('gs_kostenlefmapping', ifNull(['mapping.uitgave_categorie', 'CASE WHEN lef.lef_lead_type = "Aftersales" THEN "Aftersales" WHEN lef.lef_lead_type = "Sales" AND lef.lef_gewenst_autosoort = "Occasion" THEN "Verkoop occasion" WHEN lef.lef_lead_type = "Sales" AND lef.lef_gewenst_autosoort = "Nieuw" THEN "Verkoop nieuw" WHEN lef.lef_soort_lead = "Private lease" THEN "Private lease" ELSE NULL END'], 'AS uitgave_categorie,'))}
    ROW_NUMBER() OVER(
      PARTITION BY lef.account, lef.lef_lead_id ${ifSource('stg_sam_offertes_orders', ', lef.sam_salestraject_id')}
      ORDER BY lef.ga4_event_timestamp ASC ${ifSource('stg_sam_offertes_orders', ', lef.sam_salestraject_creatie_datum DESC')}
    ) AS lead_rank
  FROM lef_ga4 lef
  ${join("LEFT JOIN", "googleSheets", "gs_kostenlefmapping", "AS mapping ON mapping.lef_bron = lef.lead_bron AND mapping.lef_kwalificatie = lef.kwalificatie AND mapping.lef_systeem = lef.systeem AND IF(mapping.lef_source_medium IS NULL, '1', mapping.lef_source_medium) = IF(mapping.lef_source_medium IS NULL, '1', lef.session_source_medium)")}
  ${join("LEFT JOIN", "googleSheets", "gs_lef_medewerker_functie_mapping", "AS functiemapping ON functiemapping.medewerker = lef.medewerker")}
)

-- Eindresultaat: combineer verrijkte LEF+SAM rijen met Syntec, normaliseer gemeenschappelijke velden
SELECT
  IFNULL(lef.bron, syntec.bron) AS bron,
  IFNULL(lef.account, syntec.account) AS account,
  IFNULL(lef.kanaal, syntec.kanaal) AS kanaal,
  IFNULL(lef.record_date, syntec.record_date) AS record_date,
  IFNULL(lef.session_campaign, syntec.onderwerp) AS session_campaign,
  IFNULL(lef.merk_session, syntec.merk) AS merk,
  lef.session_source_medium,
  lef.user_pseudo_id,
  lef.event_name,
  lef.event_page_location,
  lef.session_landingpage_location,
  lef.session_landingpage_title,
  lef.session_device_category AS device_category,
  lef.session_geo_city,
  lef.session_geo_country AS geo_country,
  IFNULL(lef.uitgave_categorie, syntec.uitgave_categorie) AS uitgave_categorie,
  IFNULL(lef.kanaal, syntec.uitgavebron) AS uitgave_bron,

  -- LEF velden
  lef.lef_lead_id,
  lef.lef_aangemaakt_datum,
  lef.lef_afgerond_datum,
  lef.lead_bron AS lef_lead_bron,
  lef.systeem AS lef_systeem,
  lef.kwalificatie AS lef_kwalificatie,
  lef.lef_lead_type,
  lef.lef_initiatief,
  lef.lef_soort_lead,
  lef.lef_lead_omschrijving,
  lef.lef_vestiging,
  lef.lef_medewerker,
  lef.resultaat AS lef_resultaat,
  lef.afsluitreden AS lef_afsluitreden,
  lef.lef_heeft_offerte,
  lef.lef_heeft_order,
  lef.lef_ordernummer,
  lef.lef_dealernummer,
  lef.lef_gewenst_model,
  lef.lef_gewenst_merk,
  lef.lef_autosoort,
  lef.lef_gewenst_brandstof AS lef_brandstof,
  lef.lef_gewenst_bouwjaar AS lef_bouwjaar,
  lef.lef_eerste_contactpoging,
  lef.lef_laatste_status_startgesprek,
  lef.lef_deadline_gehaald,
  lef.lef_deadline_gehaald_importeur,
  lef.lef_eerste_deadline,
  lef.lef_eerste_deadline_importeur,
  lef.lef_doorlooptijd_tot_ingezien,
  lef.lef_doorlooptijd_tot_eerste_contactpoging,
  lef.lef_ingezien_datum,

  -- SAM velden
  ${ifSource("stg_sam_offertes_orders", "lef.sam_salestraject_id,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_salestraject_afgerond_datum,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_salestraject_creatie_datum,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_soort_auto,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_offerte_status,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_offerte_totaalbedrag,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_herkomst,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_offerte_id,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_getekende_offertes,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_salestraject_status_id,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_brutomarge,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_merk,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_model,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_dealer_naam,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_verkoper_naam,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_leadtraject_id,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_leadtraject_eerste_kwalificatie,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_order_id,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_aflevertraject_aantal,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_afleveringstatus,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_soort_klantcategorie,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_soort_brandstof,")}
  ${ifSource("stg_sam_offertes_orders", "lef.sam_order_datum,")}

  -- Syntec velden
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_lead_id,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_ordersoort,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.onderwerp AS syntec_onderwerp,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.vestiging AS syntec_vestiging,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.model AS syntec_model,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.kenteken AS syntec_kenteken,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_aangelegd,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_datum_gesloten,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_sluitreden,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_lead_order_id,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_klantkoppeling,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_verkoper,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_laatste_opmerking,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_contactmomenten,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_min_voor_geopend,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_order_id,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_order_status,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_date_delivery,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_customergroup,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.kanaal_groep AS syntec_kanaal_groep,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.kanaal AS syntec_kanaal,")}

FROM (SELECT * FROM lef_enriched WHERE lead_rank = 1) lef

${join("FULL OUTER JOIN", "df_staging_views", "stg_syntec_leads_orders_combined", "AS syntec ON 1=0")}
`
let refs = getRefs()
module.exports = {query, refs}
