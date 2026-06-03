/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  "Hubspot" AS bron,
  verstuurd_bericht,
  datum_bericht,
  email,
  binnen_90_dagen AS afspraak_na_mail,
  IF(binnen_90_dagen = true, factuurbedrag, 0) AS afspraak_factuurbedrag_na_mail,
  werkplaatsafspraak_datum,
  werkplaats_vestiging,
  kenteken,
  merk,
  model,
  werkplaatsafspraak_vestiging,
  COALESCE(werkplaatsafspraak_vestiging, werkplaats_vestiging) as vestiging,

FROM (
  SELECT
    ROW_NUMBER() OVER (
      PARTITION BY verstuurd_bericht, voertuig_kenteken, hubspot.email, datum_bericht 
      ORDER BY werkplaatsafspraak_datum DESC
    ) AS rank,
    COALESCE(hubspot.email, werkplaatsafspraken.email) as email,
    hubspot.* EXCEPT(email),
    werkplaatsafspraak_datum,
    IFNULL(werkplaatsafspraken.werkplaats_vestiging, hubspot.werkplaats_vestiging) as werkplaatsafspraak_vestiging,
    werkplaatsafspraken.factuurbedrag,
    IF(werkplaatsafspraak > CAST(datum_bericht AS DATE)
      AND werkplaatsafspraak <= DATE_ADD(CAST(datum_bericht AS DATE), INTERVAL 90 DAY),
       true, false) AS binnen_90_dagen,
    werkplaatsafspraak as werkplaatsafspraak_datum,
    werkplaatsafspraken.werkplaats_vestiging,
    hubspot.kenteken,
    merk,
    model,
  
  FROM
    ${ref("df_staging_views","stg_hubspot_aftersales_emailstats")} hubspot

  LEFT JOIN
    ${ref("df_staging_views","stg_ma_assign_workorder")} werkplaatsafspraken
    ON
    TRIM(REPLACE(hubspot.voertuig_kenteken, "-", "")) = TRIM(werkplaatsafspraken.kenteken)
    OR TRIM(hubspot.voertuig_id) = TRIM(werkplaatsafspraken.werkorder_voertuig_id)
    )
WHERE
  rank = 1
`
let refs = pk.getRefs()
module.exports = {query, refs}
