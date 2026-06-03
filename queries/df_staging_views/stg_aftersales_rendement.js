/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
  "Hubspot"                                                                   AS aftersales_bron,
  verstuurd_bericht                                                           AS aftersales_verstuurd_bericht,
  datum_bericht                                                               AS aftersales_datum_bericht,
  email                                                                       AS aftersales_email,
  binnen_90_dagen                                                             AS aftersales_afspraak_na_mail,
  IF(binnen_90_dagen = true, SAFE_CAST(factuurbedrag AS FLOAT64), 0)          AS aftersales_afspraak_factuurbedrag_na_mail,
  werkplaatsafspraak_datum                                                    AS aftersales_werkplaatsafspraak_datum,
  werkplaats_vestiging                                                        AS aftersales_werkplaats_vestiging,
  kenteken                                                                    AS aftersales_kenteken,
  merk                                                                        AS aftersales_merk,
  model                                                                       AS aftersales_model,
  werkplaatsafspraak_vestiging                                                AS aftersales_werkplaatsafspraak_vestiging,
  COALESCE(werkplaatsafspraak_vestiging, werkplaats_vestiging)                AS aftersales_vestiging,

FROM (
  SELECT
    ROW_NUMBER() OVER (
      PARTITION BY verstuurd_bericht, hubspot.kenteken, hubspot.email, datum_bericht
      ORDER BY werkplaatsafspraak DESC
    ) AS rank,
    email,
    hubspot.* EXCEPT(email),
    IFNULL(werkplaatsafspraken.werkplaats_vestiging, hubspot.werkplaats_vestiging) as werkplaatsafspraak_vestiging,
    werkplaatsafspraken.factuurbedrag,
    IF(werkplaatsafspraak > CAST(datum_bericht AS DATE)
      AND werkplaatsafspraak <= DATE_ADD(CAST(datum_bericht AS DATE), INTERVAL 90 DAY),
       true, false) AS binnen_90_dagen,
    werkplaatsafspraak as werkplaatsafspraak_datum,

  FROM
    ${ref("df_staging_views","stg_hubspot_aftersales_emailstats")} hubspot

  LEFT JOIN
    ${ref("df_staging_views","stg_ma_assign_workorder")} werkplaatsafspraken
    ON
    TRIM(REPLACE(hubspot.kenteken, "-", "")) = TRIM(werkplaatsafspraken.kenteken)
    OR TRIM(hubspot.voertuig_id) = TRIM(werkplaatsafspraken.werkorder_voertuig_id)
    )
WHERE
  rank = 1
  AND verstuurd_bericht IS NOT NULL
`
let refs = pk.getRefs()
module.exports = {query, refs}
