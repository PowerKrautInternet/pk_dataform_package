
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
JSON_VALUE(PAYLOAD, '$.offerteid') as OFFERTE_OFFERTEID,
  JSON_VALUE(PAYLOAD, '$.response.trajectid') as OFFERTE_TRAJECTID,
  JSON_VALUE(PAYLOAD, '$.response.offertestatusid') as OFFERTE_OFFERTESTATUSID,
  JSON_VALUE(PAYLOAD, '$.response.dealerid') as OFFERTE_DEALERID,
  JSON_VALUE(PAYLOAD, '$.response.verkoperid') as OFFERTE_VERKOPERID,
  JSON_VALUE(PAYLOAD, '$.response.relatieid') as OFFERTE_RELATIEID,
  JSON_VALUE(PAYLOAD, '$.response.iszakelijk') as OFFERTE_ISZAKELIJK,
  JSON_VALUE(PAYLOAD, '$.response.automerkid') as OFFERTE_AUTOMERKID,
  JSON_VALUE(PAYLOAD, '$.response.afleveringmodelid') as OFFERTE_AFLEVERINGMODELID,
  
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.inruilbedrag'), "") AS FLOAT64) as INRUILBEDRAG,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.korting'), "") AS FLOAT64) as KORTING,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.totaalbedrag'), "") AS FLOAT64) as OFFERTE_TOTAALBEDRAG,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.totaalbedragbtw'), "") AS FLOAT64) as TOTAALBEDRAGBTW,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.totaalbedragbpm'), "") AS FLOAT64) as TOTAALBEDRAGBPM,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoomzetconsument'), "") AS FLOAT64) as AUTOOMZETCONSUMENT,
  
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoomzetbtw'), "") AS FLOAT64) as AUTOOMZETBTW,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoomzetbpm'), "") AS FLOAT64) as AUTOOMZETBPM,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastprijsconsument'), "") AS FLOAT64) as AUTOAANGEPASTPRIJSCONSUMENT,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastrijklaar'), "") AS FLOAT64) as AUTOAANGEPASTRIJKLAAR,
  
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastexbtwprijs'), "") AS FLOAT64) as AUTOAANGEPASTEXBTWPRIJS,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastbrutoprijs'), "") AS FLOAT64) as AUTOAANGEPASTBRUTOPRIJS,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastnettoprijs'), "") AS FLOAT64) as AUTOAANGEPASTNETTOPRIJS,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastbtwprijs'), "") AS FLOAT64) as AUTOAANGEPASTBTWPRIJS,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoaangepastbpmprijs'), "") AS FLOAT64) as AUTOAANGEPASTBPMPRIJS,

  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoprijsconsument'), "") AS FLOAT64) as AUTOPRIJSCONSUMENT,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoprijsbtw'), "") AS FLOAT64) as AUTOPRIJSBTW,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autoprijsbpm'), "") AS FLOAT64) as AUTOPRIJSBPM,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autorijklaar'), "") AS FLOAT64) as AUTORIJKLAAR,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.autorijklaardealer'), "") AS FLOAT64) as AUTORIJKLAARDEALER,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.nettocatprijs'), "") AS FLOAT64) as NETTOCATPRIJS,
  
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') as OFFERTE_DTCMEDIA_CRM_ID,
  CAST(NULLIF(JSON_VALUE(PAYLOAD, '$.response.datumofferte'), "") AS DATE) as OFFERTE_DATUMOFFERTE,
  JSON_VALUE(PAYLOAD, '$.response.isleasemaatschappij') as OFFERTE_ISLEASEMAATSCHAPPIJ,
  JSON_VALUE(PAYLOAD, '$.response.automodelid') as OFFERTE_AUTOMODELID,
  JSON_VALUE(PAYLOAD, '$.response.automodelomschrijving') as OFFERTE_AUTOMODELOMSCHRIJVING,
  JSON_VALUE(PAYLOAD, '$.response.garantie') as OFFERTE_GARANTIE,
  JSON_VALUE(PAYLOAD, '$.response.automerkcode') as OFFERTE_AUTOMERKCODE,
  JSON_VALUE(PAYLOAD, '$.response.automodelcode') as OFFERTE_AUTOMODELCODE,
  JSON_VALUE(PAYLOAD, '$.response.occasionid') as OFFERTE_OCCASIONID,
  JSON_VALUE(PAYLOAD, '$.response.inruiloccasionid') as OFFERTE_INRUILOCCASIONID
  FROM ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMOffertePublisher"
      

`
let refs = pk.getRefs()
module.exports = {query, refs}
