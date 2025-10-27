/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT 
"LEF" AS bron,
* EXCEPT(week, medewerker, vestiging, sessie_conversie_bron, kanaal, lead_rank, event_timestamp, account, merk_session, gewenstMerk ${ifSource('gs_kostenlefmapping', ',lef_bron, lef_kwalificatie, lef_systeem, uitgave_bron, uitgave_merk, uitgave_categorie')} ),
${ifNull(['sessie_conversie_bron', ifSource('gs_kostenlefmapping', 'uitgave_bron')])} AS kanaal,
lef.account AS account,
${ifNull(['merk_session', 'gewenstMerk', ifSource('gs_kostenlefmapping', 'uitgave_merk')])} AS merk_session,
lef.medewerker AS medewerker,
lef.vestiging AS vestiging,
${ifSource('gs_kostenlefmapping', ifNull(['uitgave_categorie', 'CASE WHEN leadType = "Aftersales" THEN "Aftersales" WHEN leadType = "Sales" AND gewenstAutoSoort = "Occasion" THEN "Verkoop occasion" WHEN leadType = "Sales" AND gewenstAutoSoort = "Nieuw" THEN "Verkoop nieuw" WHEN soortLead = "Private lease" THEN "Private lease" ELSE NULL END'], 'AS uitgave_categorie'))} 
                                                                     
FROM(
  SELECT
  lef.account,
  pk_crm_id,
  type,
  LEFleadID,
  aangemaaktDatum,
  eersteDeadline,
  ingezienDatum,
  eersteContactpoging,
  afgerondDatum,
  aantalIngezien,
  doorlooptijdTotIngezien,
  doorlooptijdTotEersteContactpoging,
  doorlooptijdEersteContactpogingBinnenOpvolgtijdenMinuten,
  deadlineGehaald,
  aantalActiesOpLead,
  aantalNietBereikt,
  aantalOpnieuwIngepland,
  aantalDoorgezet,
  leadID,
  bron AS lead_bron,
  systeem,
  kwalificatie,
  leadType,
  initiatief,
  soortLead,
  leadOmschrijving,
  vestiging,
  medewerker,
  laatsteStatus_startGesprek,
  resultaat,
  afsluitreden,
  afsluitreden_opmerking,
  leadresultaat_datumVan,
  leadresultaat_datumTot,
  leadresultaat_verkoper,
  leadresultaat_vestiging,
  heeftOfferte,
  heeftOrder,
  ordernummer,
  dealernummer,
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
  gewenstMerk,
  gewenstModel,
  gewenstAutoSoort,
  gewenstBrandstof,
  gewenstBouwjaar,
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
  CASE
    WHEN regexp_contains(LOWER(session_medium),'whatsapp') THEN 'Whatsapp'
    WHEN regexp_contains(session_source,'dv360') 
    OR regexp_contains(session_medium,'^(.*cpm.*)$') THEN 'DV360'
    WHEN regexp_contains(session_source,'facebook|Facebook|fb|instagram|ig|meta')
    AND regexp_contains(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|.*paid.*)$') THEN 'META'
    WHEN regexp_contains(session_source,'linkedin')
    AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'LinkedIn'
    WHEN regexp_contains(session_source,'google|adwords')
    AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Google Ads'
    WHEN regexp_contains(session_source,'bing')
    AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Microsoft Ads'
    WHEN regexp_contains(session_source,'ActiveCampaign') THEN 'ActiveCampaign'
    WHEN regexp_contains(LOWER(session_source),'hs_') OR regexp_contains(LOWER(session_source),'hubspot') THEN 'Hubspot'
    ELSE NULL
  END AS sessie_conversie_bron,
  session_campaign,
  merk_session,
  kanaal,
  ${ifSource('stg_sam_offertes', 'offerte_SALESTRAJECT_TRAJECTID, DATE(offerte_SALESTRAJECT_AFGERONDDATUM) AS offerte_SALESTRAJECT_AFGERONDDATUM, DATE(offerte_SALESTRAJECT_CREATIEDATUM) AS offerte_SALESTRAJECT_CREATIEDATUM, offerte_OFFERTESTATUS_OMSCHRIJVING, offerte_OFFERTE_TOTAALBEDRAG, offerte_HERKOMST_OMSCHRIJVING, offerte_OFFERTE_OFFERTEID, getekende_offertes, offerte_SALESTRAJECT_TRAJECTSTATUSID, offerte_OFFERTEVTR_BRUTOMARGEBEDRAG, offerte_MERK_OMSCHRIJVING, offerte_AFLEVERINGMODEL_OMSCHRIJVING, offerte_DEALER_NAAM, offerte_VERKOPER_NAAM,')}
  ROW_NUMBER() OVER(PARTITION BY lef.account, LEFleadID ORDER BY event_timestamp ASC) AS lead_rank
  
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

${join("FULL OUTER JOIN", "df_staging_views", "stg_sam_offertes", "AS SAM ON offerte_LEADTRAJECT_EXTERNLEADID = LEFleadID")}
) lef
${join("LEFT JOIN", "googleSheets", "gs_kostenlefmapping", "AS mapping ON mapping.lef_bron = lef.lead_bron AND mapping.lef_kwalificatie = lef.kwalificatie AND mapping.lef_systeem = lef.systeem AND lef.vestiging = mapping.lef_vestiging")}

${join("LEFT JOIN", "googleSheets", "gs_lef_medewerker_functie_mapping", "AS functiemapping ON functiemapping.medewerker = lef.medewerker")}

CROSS JOIN 
(
  WITH weekly_metrics AS (
  SELECT
    EXTRACT(WEEK FROM aangemaaktDatum) AS week,
    AVG(
      CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(0)] AS FLOAT64) * 24
      + CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(1)] AS FLOAT64)
      + CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(2)] AS FLOAT64) / 60
      + CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(3)] AS FLOAT64) / 3600
    ) AS avg_doorlooptijd_hours,
    SAFE_DIVIDE(
      COUNT(DISTINCT IF(deadlineGehaald = 'true', LEFleadID, NULL)),
      COUNT(DISTINCT LEFleadID)
    ) AS deals_pct
  FROM ${ref("df_rawdata_views", "lef_leads")}
  GROUP BY week
)

SELECT
  AVG(avg_doorlooptijd_hours) AS mean_doorlooptijd_hours,
  STDDEV(avg_doorlooptijd_hours) AS std_doorlooptijd_hours,
  AVG(deals_pct) AS mean_deals,
  STDDEV(deals_pct) AS std_deals
FROM weekly_metrics) mean_stddev

LEFT JOIN 
(
  WITH weekly_metrics AS (
  SELECT
    medewerker,
    vestiging,
    EXTRACT(WEEK FROM aangemaaktDatum) AS week,
    COUNT(DISTINCT LEFleadID) AS leads_count,
  FROM ${ref("df_rawdata_views", "lef_leads")}
  GROUP BY medewerker, vestiging, week
)

SELECT
  medewerker,
  vestiging,
  week,
  AVG(leads_count) OVER (PARTITION BY medewerker, vestiging) AS mean_leads,
  STDDEV(leads_count) OVER (PARTITION BY medewerker, vestiging) AS std_leads,
FROM weekly_metrics) mean_stddev_leads
ON mean_stddev_leads.medewerker = lef.medewerker
AND mean_stddev_leads.vestiging = lef.vestiging
AND mean_stddev_leads.week = EXTRACT(WEEK FROM tot.aangemaaktDatum)

WHERE lead_rank = 1
`
let refs = getRefs()
module.exports = {query, refs}
