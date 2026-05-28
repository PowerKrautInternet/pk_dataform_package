/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
  offerte_SALESTRAJECT_DTCMEDIA_CRM_ID,
  offerte_SALESTRAJECT_TRAJECTID,
  SAFE.DATE(offerte_SALESTRAJECT_AFGERONDDATUM) AS offerte_SALESTRAJECT_AFGERONDDATUM,
  SAFE.DATE(offerte_SALESTRAJECT_CREATIEDATUM) AS offerte_SALESTRAJECT_CREATIEDATUM,
  CASE
    WHEN offerte_SALESTRAJECT_SOORTAUTO = '1' THEN 'Nieuw'
    WHEN offerte_SALESTRAJECT_SOORTAUTO = '2' THEN 'Occasion'
    ELSE NULL
  END AS offerte_SALESTRAJECT_SOORTAUTO,
  offerte_OFFERTESTATUS_OMSCHRIJVING,
  offerte_OFFERTE_TOTAALBEDRAG,
  offerte_AUTOPRIJSCONSUMENT,
  ${pk.ifSource("sam_herkomst", `offerte_HERKOMST_OMSCHRIJVING,`)}
  offerte_OFFERTE_OFFERTEID,
  offerte_OFFERTE_DATUMOFFERTE,
  getekende_offertes,
  offerte_SALESTRAJECT_TRAJECTSTATUSID,
  ${pk.ifSource("sam_offerte_vtr", `offerte_OFFERTEVTR_BRUTOMARGEBEDRAG,`)}
  offerte_MERK_OMSCHRIJVING,
  offerte_AFLEVERINGMODEL_OMSCHRIJVING,
  offerte_DEALER_NAAM,
  offerte_VERKOPER_VOORVOEGSEL,
  offerte_VERKOPER_NAAM,
  offerte_LEADTRAJECT_TRAJECTID,
  offerte_LEADTRAJECT_EERSTEKWALIFICATIE,
  ${pk.ifSource("sam_trajects_extern", `offerte_LEADTRAJECT_EXTERNLEADID,`)}
  order_TRAJECT_TRAJECTID,
  order_AFLEVERTRAJECT_AANTAL,
  afleveringstatus_omschrijving,
  SOORTKLANTCATEGORIE_OMSCHRIJVING,
  SOORTBRANDSTOF_OMSCHRIJVING,
  ORDERDATUM

FROM ${ref("stg_sam_offertes")} offertes

LEFT JOIN ${ref("stg_sam_orders")} orders
ON offerte_SALESTRAJECT_TRAJECTID = order_TRAJECT_TRAJECTID
AND offerte_SALESTRAJECT_DTCMEDIA_CRM_ID = orders.DTCMEDIA_CRM_ID
`
let refs = pk.getRefs()
module.exports = {query, refs}
