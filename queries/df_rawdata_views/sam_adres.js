
/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


SELECT
ADRES_RELATIEID,
ADRES_DTCMEDIA_CRM_ID,
MAX(ADRES_ADRESID) as ADRES_ADRESID,
MAX(ADRES_SOORTADRESID) as ADRES_SOORTADRESID,

MAX(ADRES_HUISNUMMER) as ADRES_HUISNUMMER,
MAX(ADRES_HUISNRTOEVOEGING) as ADRES_HUISNRTOEVOEGING,
MAX(ADRES_POSTCODE) as ADRES_POSTCODE,
MAX(ADRES_ISSTANDAARD) as ADRES_ISSTANDAARD,

MAX(ADRES_STRAAT) as ADRES_STRAAT,
MAX(ADRES_PLAATS) as ADRES_PLAATS,
MAX(ADRES_LAND) as ADRES_LAND,
MAX(ADRES_ACTIEF) as ADRES_ACTIEF

FROM (
      SELECT
          JSON_VALUE(PAYLOAD, '$.dtcmedia_crm_id') AS ADRES_DTCMEDIA_CRM_ID,
          JSON_VALUE(PAYLOAD, '$.type') AS type,
          JSON_VALUE(PAYLOAD, '$.adresid') AS ADRES_ADRESID,
          JSON_VALUE(PAYLOAD, '$.response.actief') AS ADRES_ACTIEF,
          JSON_VALUE(PAYLOAD, '$.response.huisnrtoevoeging') AS ADRES_HUISNRTOEVOEGING,
          JSON_VALUE(PAYLOAD, '$.response.huisnummer') AS ADRES_HUISNUMMER,
          JSON_VALUE(PAYLOAD, '$.response.isstandaard') AS ADRES_ISSTANDAARD,
          JSON_VALUE(PAYLOAD, '$.response.isstandaardwrite') AS ADRES_ISSTANDAARDWRITE,
          JSON_VALUE(PAYLOAD, '$.response.land') AS ADRES_LAND,
          JSON_VALUE(PAYLOAD, '$.response.plaats') AS ADRES_PLAATS,
          JSON_VALUE(PAYLOAD, '$.response.postcode') AS ADRES_POSTCODE,
          JSON_VALUE(PAYLOAD, '$.response.relatieid') AS ADRES_RELATIEID,
          JSON_VALUE(PAYLOAD, '$.response.soortadresid') AS ADRES_SOORTADRESID,
          JSON_VALUE(PAYLOAD, '$.response.straat') AS ADRES_STRAAT,
          JSON_VALUE(PAYLOAD, '$.response.zoekplaats') AS ADRES_ZOEKPLAATS

      FROM ${ref("odbcDataProducer_lasttransaction")}
      WHERE PUBLISHER = "SAMAdresPublisher"
)

WHERE ADRES_ISSTANDAARD = "T" AND ADRES_ACTIEF = 'T'
GROUP BY ADRES_RELATIEID, ADRES_DTCMEDIA_CRM_ID
`
let refs = pk.getRefs()
module.exports = {query, refs}
