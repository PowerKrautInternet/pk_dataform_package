
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  JSON_VALUE(PAYLOAD, '$.gebruikerid') AS VERKOPER_GEBRUIKERID,
  JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS VERKOPER_DTCMEDIA_CRM_ID,
  JSON_VALUE(PAYLOAD, '$.type') AS VERKOPER_TYPE,
  JSON_VALUE(PAYLOAD, '$.response.naam') AS VERKOPER_NAAM,
  JSON_VALUE(PAYLOAD, '$.response.voorvoegsel') AS VERKOPER_VOORVOEGSEL,
  JSON_VALUE(PAYLOAD, '$.response.initialen') AS VERKOPER_INITIALEN,
  JSON_VALUE(PAYLOAD, '$.response.email') AS VERKOPER_EMAIL,
  JSON_VALUE(PAYLOAD, '$.response.telefoon1') AS VERKOPER_TELEFOON1,
  JSON_VALUE(PAYLOAD, '$.response.telefoon2') AS VERKOPER_TELEFOON2,
  JSON_VALUE(PAYLOAD, '$.response.telefoon3') AS VERKOPER_TELEFOON3,
  JSON_VALUE(PAYLOAD, '$.response.straat') AS VERKOPER_STRAAT,
  JSON_VALUE(PAYLOAD, '$.response.huisnummer') AS VERKOPER_HUISNUMMER,
  JSON_VALUE(PAYLOAD, '$.response.postcode') AS VERKOPER_POSTCODE,
  JSON_VALUE(PAYLOAD, '$.response.plaats') AS VERKOPER_PLAATS,
  JSON_VALUE(PAYLOAD, '$.response.actief') AS VERKOPER_ACTIEF,
  JSON_VALUE(PAYLOAD, '$.response.bevoegdheden') AS VERKOPER_BEVOEGDHEDEN,
  JSON_VALUE(PAYLOAD, '$.response.soortgebruikerid') AS VERKOPER_SOORTGEBRUIKERID,
  JSON_VALUE(PAYLOAD, '$.response.tooninrapport') AS VERKOPER_TOONINRAPPORT,
  JSON_VALUE(PAYLOAD, '$.response.werkdagen') AS VERKOPER_WERKDAGEN,
  JSON_VALUE(PAYLOAD, '$.response.externid') AS VERKOPER_EXTERNID,
  JSON_VALUE(PAYLOAD, '$.response.externcode') AS VERKOPER_EXTERNCODE,
  JSON_VALUE(PAYLOAD, '$.response.csbuser') AS VERKOPER_CSBUSER,
  JSON_VALUE(PAYLOAD, '$.response.csbpassword') AS VERKOPER_CSBPASSWORD,
  JSON_VALUE(PAYLOAD, '$.response.wachtwoord') AS VERKOPER_WACHTWOORD,
  JSON_VALUE(PAYLOAD, '$.response.passwordhash') AS VERKOPER_PASSWORDHASH,
  JSON_VALUE(PAYLOAD, '$.response.password_expirationdate') AS VERKOPER_PASSWORD_EXPIRATIONDATE,
  JSON_VALUE(PAYLOAD, '$.response.ww_datumaangepast') AS VERKOPER_WW_DATUMAANGEPAST,
  JSON_VALUE(PAYLOAD, '$.response.achmeasvcgebruikersnaam') AS VERKOPER_ACHMEASVCGEBRUIKERSNAAM,
  JSON_VALUE(PAYLOAD, '$.response.aldwebquotergebruikersnaam') AS VERKOPER_ALDWEBQUOTERGEBRUIKERSNAAM,
  JSON_VALUE(PAYLOAD, '$.response.autotelexgebruikersnaam') AS VERKOPER_AUTOTELEXGEBRUIKERSNAAM,
  JSON_VALUE(PAYLOAD, '$.response.carmeleonverkoperid') AS VERKOPER_CARMELEONVERKOPERID
FROM 
      ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMGebruikerPublisher"

`
let refs = pk.getRefs()
module.exports = {query, refs}
