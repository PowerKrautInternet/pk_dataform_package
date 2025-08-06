/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT 
"LEF" AS bron,
* EXCEPT(sessie_conversie_bron, kanaal, lead_rank, event_timestamp, account ${ifSource('gs_kostenlefmapping', ',lef_bron, lef_kwalificatie, lef_systeem, uitgave_bron, uitgave_merk')} ),
IFNULL(sessie_conversie_bron, kanaal) AS kanaal,
lef.account AS account,
${ifNull(['merk_session', 'gewenstMerk', ifSource('gs_kostenlefmapping', 'uitgave_merk')])} AS merk_session,
${ifSource('gs_kostenlefmapping', ifNull(['uitgave_categorie', 'CASE WHEN leadType = "Aftersales" THEN "Aftersales" WHEN leadType = "Sales" AND gewenstAutoSoort = "Occasion" THEN "Verkoop occasion" WHEN leadType = "Sales" AND gewenstAutoSoort = "Nieuw" THEN "Verkoop Nieuw" WHEN soortLead = "Private lease" THEN "Private lease" ELSE NULL END'], 'AS uitgave_categorie'))} 
                                                                     
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
    WHEN regexp_contains(session_source,'dv360') 
    OR regexp_contains(session_medium,'^(.*cpm.*)$') THEN 'DV360'
    WHEN regexp_contains(session_source,'facebook|Facebook|fb|instagram|ig|meta')
    AND regexp_contains(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|.*paid.*)$') THEN 'Facebook'
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
) lef
${join("LEFT JOIN", "gs_kostenlefmapping", "AS mapping ON mapping.lef_bron = lef.lead_bron AND mapping.lef_kwalificatie = lef.kwalificatie AND mapping.lef_systeem = lef.systeem")}
WHERE lead_rank = 1
`
let refs = getRefs()
module.exports = {query, refs}
