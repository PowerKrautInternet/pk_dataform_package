/*config*/
const {join, ref, getRefs, ifSource, ifNull, channelCase} = require("../../sources");
let query = `

SELECT
"LEF" AS bron,
* EXCEPT( ${ifSource("stg_lef_leads_avg", "week,")} medewerker, vestiging, sessie_conversie_bron, kanaal, lead_rank, event_timestamp, account, merk_session ${ifSource('gs_kostenlefmapping', ',lef_bron, lef_kwalificatie, lef_systeem, uitgave_bron, uitgave_merk, uitgave_categorie')} ),
${ifNull(['sessie_conversie_bron', ifSource('gs_kostenlefmapping', 'uitgave_bron')])} AS kanaal,
lef.account AS account,
${ifNull([ifSource('stg_sam_offertes_orders', 'sam_merk'), 'lef_gewenst_merk', 'merk_session', ifSource('gs_kostenlefmapping', 'uitgave_merk')])} AS merk_session,
${ifNull([ifSource('stg_sam_offertes_orders', 'sam_model'), 'lef_gewenst_model'])} AS gewenst_model,
${ifNull([ifSource('stg_sam_offertes_orders', 'sam_soort_auto'), 'lef_gewenst_autosoort'])} AS gewenst_autosoort,
lef.medewerker AS medewerker,
lef.vestiging AS vestiging,
${ifSource('gs_kostenlefmapping', ifNull(['uitgave_categorie', 'CASE WHEN lef_lead_type = "Aftersales" THEN "Aftersales" WHEN lef_lead_type = "Sales" AND lef_gewenst_autosoort = "Occasion" THEN "Verkoop occasion" WHEN lef_lead_type = "Sales" AND lef_gewenst_autosoort = "Nieuw" THEN "Verkoop nieuw" WHEN lef_soort_lead = "Private lease" THEN "Private lease" ELSE NULL END'], 'AS uitgave_categorie'))}

FROM(
  SELECT
  lef.account,
  pk_crm_id,
  type,
  LEFleadID AS lef_lead_id,
  aangemaaktDatum AS lef_aangemaakt_datum,
  eersteDeadline AS lef_eerste_deadline,
  eersteDeadlineImporteur AS lef_eerste_deadline_importeur,
  ingezienDatum AS lef_ingezien_datum,
  eersteContactpoging AS lef_eerste_contactpoging,
  afgerondDatum AS lef_afgerond_datum,
  aantalIngezien,
  doorlooptijdTotIngezien AS lef_doorlooptijd_tot_ingezien,
  doorlooptijdTotEersteContactpoging AS lef_doorlooptijd_tot_eerste_contactpoging,
  doorlooptijdEersteContactpogingBinnenOpvolgtijdenMinuten,
  deadlineGehaald AS lef_deadline_gehaald,
  deadlineGehaaldImporteur AS lef_deadline_gehaald_importeur,
  aantalActiesOpLead,
  aantalNietBereikt,
  aantalOpnieuwIngepland,
  aantalDoorgezet,
  leadID,
  bron AS lead_bron,
  systeem,
  kwalificatie,
  leadType AS lef_lead_type,
  initiatief AS lef_initiatief,
  soortLead AS lef_soort_lead,
  leadOmschrijving AS lef_lead_omschrijving,
  vestiging,
  medewerker,
  laatsteStatus_startGesprek AS lef_laatste_status_startgesprek,
  resultaat,
  afsluitreden,
  afsluitreden_opmerking,
  leadresultaat_datumVan,
  leadresultaat_datumTot,
  leadresultaat_verkoper,
  leadresultaat_vestiging,
  heeftOfferte AS lef_heeft_offerte,
  heeftOrder AS lef_heeft_order,
  ordernummer AS lef_ordernummer,
  dealernummer AS lef_dealernummer,
  laatsteMutatieDatum,
  achternaam,
  tussenvoegsel,
  voorletters,
  geslachtType,
  relatieType,
  postcode,
  huisnummer,
  huisnummertoevoeging,
  straat,
  plaats,
  email,
  telefoon,
  klantinfo_bewaartermijn,
  gewenstMerk AS lef_gewenst_merk,
  gewenstModel AS lef_gewenst_model,
  gewenstAutoSoort AS lef_gewenst_autosoort,
  gewenstBrandstof AS lef_gewenst_brandstof,
  gewenstBouwjaar AS lef_gewenst_bouwjaar,
  gewenstUitvoering,
  gewenstKenteken,
  huidigMerk,
  huidigModel,
  huidigUitvoering,
  huidigBrandstof,
  huidigKenteken,
  huidigBouwjaar,
  RECEIVEDON,
  ACTION,
  google_clientid,
  user_pseudo_id,
  event_date,
  event_timestamp,
  event_name,
  event_page_location,
  session_landingpage_location,
  session_landingpage_title,
  session_device_category,
  session_geo_city,
  session_source,
  session_medium,
  session_source_medium,
  ${channelCase('session_source', 'session_medium')} AS sessie_conversie_bron,
  session_campaign,
  merk_session,
  kanaal,
  ${ifNull(["DATE(lef.aangemaaktDatum)", ifSource('stg_sam_offertes_orders', "DATE(offerte_SALESTRAJECT_CREATIEDATUM)")], "AS record_date,")}
  ${ifSource('stg_sam_offertes_orders',
    `offerte_SALESTRAJECT_TRAJECTID AS sam_salestraject_id,
    DATE(offerte_SALESTRAJECT_AFGERONDDATUM) AS sam_salestraject_afgerond_datum,
    DATE(offerte_SALESTRAJECT_CREATIEDATUM) AS sam_salestraject_creatie_datum,
    offerte_SALESTRAJECT_SOORTAUTO AS sam_soort_auto,
    offerte_OFFERTESTATUS_OMSCHRIJVING AS sam_offerte_status,
    offerte_OFFERTE_TOTAALBEDRAG AS sam_offerte_totaalbedrag,
    offerte_HERKOMST_OMSCHRIJVING AS sam_herkomst,
    offerte_OFFERTE_OFFERTEID AS sam_offerte_id,
    getekende_offertes AS sam_getekende_offertes,
    offerte_SALESTRAJECT_TRAJECTSTATUSID AS sam_salestraject_status_id,
    offerte_OFFERTEVTR_BRUTOMARGEBEDRAG AS sam_brutomarge,
    offerte_MERK_OMSCHRIJVING AS sam_merk,
    offerte_AFLEVERINGMODEL_OMSCHRIJVING AS sam_model,
    offerte_DEALER_NAAM AS sam_dealer_naam,
    CONCAT(IFNULL(offerte_VERKOPER_VOORVOEGSEL, ''), ' ', IFNULL(offerte_VERKOPER_NAAM, '')) AS sam_verkoper_naam,
    offerte_LEADTRAJECT_TRAJECTID AS sam_leadtraject_id,
    offerte_LEADTRAJECT_EERSTEKWALIFICATIE AS sam_leadtraject_eerste_kwalificatie,
    order_TRAJECT_TRAJECTID AS sam_order_id,
    order_AFLEVERTRAJECT_AANTAL AS sam_aflevertraject_aantal,
    afleveringstatus_omschrijving AS sam_afleveringstatus,
    SOORTKLANTCATEGORIE_OMSCHRIJVING AS sam_soort_klantcategorie,
    SOORTBRANDSTOF_OMSCHRIJVING AS sam_soort_brandstof,
    ORDERDATUM AS sam_order_datum,
    `)}
  ROW_NUMBER() OVER(PARTITION BY lef.account, LEFleadID ${ifSource('stg_sam_offertes_orders', ', offerte_SALESTRAJECT_TRAJECTID')} ORDER BY event_timestamp ASC ${ifSource('stg_sam_offertes_orders', ', offerte_SALESTRAJECT_CREATIEDATUM DESC')} ) AS lead_rank

FROM
  ${ref("df_rawdata_views", "lef_leads")} lef
LEFT JOIN
(SELECT
*
FROM
  ${ref("df_staging_tables", "stg_ga4_events_sessies")}
WHERE event_name = "session_start"
) kanalen
ON TRIM(lef.google_clientid) = TRIM(kanalen.user_pseudo_id) AND lef.account = kanalen.account

${join("FULL OUTER JOIN", "df_staging_views", "stg_sam_offertes_orders", "AS SAM ON offerte_LEADTRAJECT_EXTERNLEADID = LEFleadID AND offerte_SALESTRAJECT_DTCMEDIA_CRM_ID = pk_crm_id")}
) lef
${join("LEFT JOIN", "googleSheets", "gs_kostenlefmapping", "AS mapping ON mapping.lef_bron = lef.lead_bron AND mapping.lef_kwalificatie = lef.kwalificatie AND mapping.lef_systeem = lef.systeem AND IF(mapping.lef_source_medium IS NULL, '1', mapping.lef_source_medium)  = IF(mapping.lef_source_medium IS NULL, '1', lef.session_source_medium)")}

${join("LEFT JOIN", "googleSheets", "gs_lef_medewerker_functie_mapping", "AS functiemapping ON functiemapping.medewerker = lef.medewerker")}

${join("CROSS JOIN", "df_staging_views", "stg_lef_leadopvolging_avg", "AS mean_stddev")}

${join("LEFT JOIN", "df_staging_views", "stg_lef_leads_avg", `AS mean_stddev_leads
ON mean_stddev_leads.medewerker = lef.medewerker
AND mean_stddev_leads.vestiging = lef.vestiging
AND mean_stddev_leads.week = EXTRACT(WEEK FROM lef.lef_aangemaakt_datum)`)}

WHERE lead_rank = 1
`
let refs = getRefs()
module.exports = {query, refs}
