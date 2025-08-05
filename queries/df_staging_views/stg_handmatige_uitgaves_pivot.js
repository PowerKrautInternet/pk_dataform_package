/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT uitgave_bron,
uitgave_merk,
uitgave_categorie,
date AS record_datum,
uitgaven AS uitgaven,
FROM(
SELECT uitgave_bron,
                 uitgave_merk,
                 uitgave_categorie,
                 PARSE_DATE('%Y%m%d', CONCAT(jaar, maand)) as jaar_maand,
                 uitgaven,
                 GENERATE_DATE_ARRAY(
                         PARSE_DATE('%Y%m%d', CONCAT(jaar, maand)),
                         LAST_DAY(PARSE_DATE('%Y%m%d', CONCAT(jaar, maand)), MONTH)
                 )                                         as dates

          FROM (SELECT uitgave_bron,
                       uitgave_merk,
                       uitgave_categorie,
                       jaar,
                       [ STRUCT ('0101' as maand,
                           jan AS uitgaven),
                           STRUCT ('0201' as maand,
                               feb AS uitgaven),
                           STRUCT ('0301' as maand,
                               mrt AS uitgaven),
                           STRUCT ('0401' as maand,
                               apr AS uitgaven),
                           STRUCT ('0501' as maand,
                               mei AS uitgaven),
                           STRUCT ('0601' as maand,
                               jun AS uitgaven),
                           STRUCT ('0701' as maand,
                               jul AS uitgaven),
                           STRUCT ('0801' as maand,
                               aug AS uitgaven),
                           STRUCT ('0901' as maand,
                               sep AS uitgaven),
                           STRUCT ('1001' as maand,
                               okt AS uitgaven),
                           STRUCT ('1101' as maand,
                               nov AS uitgaven),
                           STRUCT ('1201' as maand,
                               dec AS uitgaven)
                           ] AS ROW

                FROM (SELECT *
                      FROM ${ref("gs_handmatigekosten")})) as uitgaven_totaal
                   CROSS JOIN
               UNNEST(uitgaven_totaal.row) AS r) as date_mapping
             CROSS JOIN
         UNNEST(dates) as date

`
let refs = pk.getRefs()
module.exports = {query, refs}
