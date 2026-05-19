/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");

let query = `

SELECT
  -- Uniforme velden
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.bron"),               ifSource("stg_syntec_leads_orders_combined", "syntec.bron")])} AS bron,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.account"),            ifSource("stg_syntec_leads_orders_combined", "syntec.account")])} AS account,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.kanaal"),             ifSource("stg_syntec_leads_orders_combined", "syntec.kanaal")])} AS kanaal,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.record_date"),        ifSource("stg_syntec_leads_orders_combined", "syntec.record_date")])} AS record_date,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.merk_session"),       ifSource("stg_syntec_leads_orders_combined", "syntec.merk")])} AS merk,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.session_campaign"),   ifSource("stg_syntec_leads_orders_combined", "syntec.onderwerp")])} AS session_campaign,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.lef_lead_id"),        ifSource("stg_syntec_leads_orders_combined", "CAST(syntec.syntec_lead_id AS STRING)")])} AS lead_id,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.lef_aangemaakt_datum"), ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_aangelegd")])} AS aangemaakt_datum,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.lef_afgerond_datum"), ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_datum_gesloten")])} AS afgerond_datum,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.vestiging"),          ifSource("stg_syntec_leads_orders_combined", "syntec.vestiging")])} AS vestiging,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.medewerker"),         ifSource("stg_syntec_leads_orders_combined", "syntec.syntec_verkoper")])} AS medewerker,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.gewenst_model"),      ifSource("stg_syntec_leads_orders_combined", "syntec.model")])} AS model,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.uitgave_categorie"),  ifSource("stg_syntec_leads_orders_combined", "syntec.uitgave_categorie")])} AS uitgave_categorie,
  ${ifNull([ifSource("stg_lef_leads_agg", "lef.kanaal"),             ifSource("stg_syntec_leads_orders_combined", "syntec.uitgavebron")])} AS uitgave_bron,

  -- GA4 sessie velden (alleen LEF)
  ${ifSource("stg_lef_leads_agg", "lef.session_source_medium,")}
  ${ifSource("stg_lef_leads_agg", "lef.event_name,")}
  ${ifSource("stg_lef_leads_agg", "lef.event_page_location,")}
  ${ifSource("stg_lef_leads_agg", "lef.session_landingpage_location,")}
  ${ifSource("stg_lef_leads_agg", "lef.session_landingpage_title,")}
  ${ifSource("stg_lef_leads_agg", "lef.session_device_category,")}
  ${ifSource("stg_lef_leads_agg", "lef.session_geo_city,")}
  ${ifSource("stg_lef_leads_agg", "lef.google_clientid AS user_pseudo_id,")}

  -- LEF velden
  ${ifSource("stg_lef_leads_agg", "lef.lef_lead_id,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_aangemaakt_datum,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_afgerond_datum,")}
  ${ifSource("stg_lef_leads_agg", "lef.lead_bron AS lef_lead_bron,")}
  ${ifSource("stg_lef_leads_agg", "lef.systeem AS lef_systeem,")}
  ${ifSource("stg_lef_leads_agg", "lef.kwalificatie AS lef_kwalificatie,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_lead_type,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_initiatief,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_soort_lead,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_lead_omschrijving,")}
  ${ifSource("stg_lef_leads_agg", "lef.vestiging AS lef_vestiging,")}
  ${ifSource("stg_lef_leads_agg", "lef.medewerker AS lef_medewerker,")}
  ${ifSource("stg_lef_leads_agg", "lef.resultaat AS lef_resultaat,")}
  ${ifSource("stg_lef_leads_agg", "lef.afsluitreden AS lef_afsluitreden,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_heeft_offerte,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_heeft_order,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_ordernummer,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_dealernummer,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_gewenst_model,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_gewenst_merk,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_gewenst_autosoort,")}
  ${ifSource("stg_lef_leads_agg", "lef.gewenst_model,")}
  ${ifSource("stg_lef_leads_agg", "lef.gewenst_autosoort,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_gewenst_brandstof AS lef_brandstof,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_gewenst_bouwjaar AS lef_bouwjaar,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_eerste_contactpoging,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_laatste_status_startgesprek,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_deadline_gehaald,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_deadline_gehaald_importeur,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_eerste_deadline,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_eerste_deadline_importeur,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_doorlooptijd_tot_ingezien,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_doorlooptijd_tot_eerste_contactpoging,")}
  ${ifSource("stg_lef_leads_agg", "lef.lef_ingezien_datum,")}
  ${ifSource("stg_lef_leadopvolging_avg", "lef.mean_doorlooptijd_hours,")}
  ${ifSource("stg_lef_leadopvolging_avg", "lef.std_doorlooptijd_hours,")}
  ${ifSource("stg_lef_leadopvolging_avg", "lef.mean_deals,")}
  ${ifSource("stg_lef_leadopvolging_avg", "lef.std_deals,")}
  ${ifSource("stg_lef_leads_avg", "lef.mean_leads,")}
  ${ifSource("stg_lef_leads_avg", "lef.std_leads,")}

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

FROM UNNEST(ARRAY<INT64>[]) AS _stub
${join("FULL OUTER JOIN", "df_staging_views", "stg_lef_leads_agg", "AS lef ON 1=0")}
${join("FULL OUTER JOIN", "df_staging_views", "stg_syntec_leads_orders_combined", "AS syntec ON 1=0")}
`
let refs = getRefs()
module.exports = {query, refs}
