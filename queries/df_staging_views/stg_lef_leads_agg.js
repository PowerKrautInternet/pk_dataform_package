/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

SELECT
  "LEF" AS bron,
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
  event_name,
  event_page_location,
  session_landingpage_location,
  session_landingpage_title,
  session_device_category,
  session_geo_city,
  session_source,
  session_medium,
  session_source_medium,
  session_campaign,
  merk_session,
  kanaal
  
FROM
  ${ref("df_rawdata_views", "lef_leads")} lef
LEFT JOIN 
(SELECT
*
FROM
  ${ref("df_staging_tables", "stg_ga4_events_sessies")} 
WHERE event_name = "session_start"
) kanalen
ON TRIM(lef.google_clientid) = TRIM(kanalen.user_pseudo_id)

`
let refs = getRefs()
module.exports = {query, refs}
