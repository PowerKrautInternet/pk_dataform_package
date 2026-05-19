/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT
  IFNULL(lef.bron, syntec.bron) AS bron,
  IFNULL(lef.account, syntec.account) AS account,
  IFNULL(lef.kanaal, syntec.kanaal) AS kanaal,
  IFNULL(lef.record_date, syntec.record_date) AS record_date,
  IFNULL(lef.merk_session, syntec.merk) AS merk,
  IFNULL(lef.session_campaign, syntec.onderwerp) AS session_campaign,
  IFNULL(lef.lef_lead_id, CAST(syntec.syntec_lead_id AS STRING)) AS lead_id,
  IFNULL(lef.lef_aangemaakt_datum, syntec.syntec_aangelegd) AS aangemaakt_datum,
  IFNULL(lef.lef_afgerond_datum, syntec.syntec_datum_gesloten) AS afgerond_datum,
  IFNULL(lef.vestiging, syntec.vestiging) AS vestiging,
  IFNULL(lef.medewerker, syntec.syntec_verkoper) AS medewerker,
  IFNULL(lef.gewenst_model, syntec.model) AS model,
  IFNULL(lef.uitgave_categorie, syntec.uitgave_categorie) AS uitgave_categorie,
  IFNULL(lef.kanaal, syntec.uitgavebron) AS uitgave_bron,

  -- GA4 sessie velden (alleen LEF)
  lef.session_source_medium,
  lef.event_name,
  lef.event_page_location,
  lef.session_landingpage_location,
  lef.session_landingpage_title,
  lef.session_device_category,
  lef.session_geo_city,
  lef.google_clientid AS user_pseudo_id,

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
  lef.vestiging AS lef_vestiging,
  lef.medewerker AS lef_medewerker,
  lef.resultaat AS lef_resultaat,
  lef.afsluitreden AS lef_afsluitreden,
  lef.lef_heeft_offerte,
  lef.lef_heeft_order,
  lef.lef_ordernummer,
  lef.lef_dealernummer,
  lef.lef_gewenst_model,
  lef.lef_gewenst_merk,
  lef.lef_gewenst_autosoort,
  lef.gewenst_model,
  lef.gewenst_autosoort,
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
  lef.mean_doorlooptijd_hours,
  lef.std_doorlooptijd_hours,
  lef.mean_deals,
  lef.std_deals,
  lef.mean_leads,
  lef.std_leads,

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
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.kanaal AS syntec_kanaal,")}
  ${ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_kanaal_groep,")}
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

FROM ${ref("df_staging_views", "stg_lef_leads_agg")} lef

${join("FULL OUTER JOIN", "df_staging_views", "stg_syntec_leads_orders_combined", "AS syntec ON 1=0")}
`
let refs = getRefs()
module.exports = {query, refs}
