/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT account,
        soort_conversie,
           kanaal,
           merk,
           target                    as month_target,
           date                                          as record_datum,
           CAST(target / ARRAY_LENGTH(dates) as FLOAT64) as day_target

    FROM (SELECT account, 
              soort_conversie,
                 kanaal,
                 merk,
                 PARSE_DATE('%Y%m%d', CONCAT(jaar, maand)) as jaar_maand,
                 cast(regexp_replace(target, r',', '.') as float64) as target, --Voor 'nederlandse' getallen in gsheet gebruikt hij soms de , ipv de . en dat kan bigquery nie hebben
                 GENERATE_DATE_ARRAY(
                         PARSE_DATE('%Y%m%d', CONCAT(jaar, maand)),
                         LAST_DAY(PARSE_DATE('%Y%m%d', CONCAT(jaar, maand)), MONTH)
                 )                                         as dates

          FROM (SELECT account,
                        soort_conversie,
                       kanaal,
                       merk,
                       jaar,
                       [ STRUCT ('0101' as maand,
                           jan AS TARGET),
                           STRUCT ('0201' as maand,
                               feb AS TARGET),
                           STRUCT ('0301' as maand,
                               mrt AS TARGET),
                           STRUCT ('0401' as maand,
                               apr AS TARGET),
                           STRUCT ('0501' as maand,
                               mei AS TARGET),
                           STRUCT ('0601' as maand,
                               jun AS TARGET),
                           STRUCT ('0701' as maand,
                               jul AS TARGET),
                           STRUCT ('0801' as maand,
                               aug AS TARGET),
                           STRUCT ('0901' as maand,
                               sep AS TARGET),
                           STRUCT ('1001' as maand,
                               okt AS TARGET),
                           STRUCT ('1101' as maand,
                               nov AS TARGET),
                           STRUCT ('1201' as maand,
                               dec AS TARGET)
                           ] AS ROW

                FROM (SELECT *
                      FROM ${ref("df_googlesheets_tables", "gs_conversie_targets")})) as targets
                   CROSS JOIN
               UNNEST(targets.row) AS r) as date_mapping
             CROSS JOIN
         UNNEST(dates) as date

`
let refs = pk.getRefs()
module.exports = {query, refs}