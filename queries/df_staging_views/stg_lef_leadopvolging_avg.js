/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
WITH weekly_metrics AS (
  SELECT
    EXTRACT(WEEK FROM aangemaaktDatum) AS week,
    AVG(
      CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(0)] AS FLOAT64) * 24
      + CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(1)] AS FLOAT64)
      + CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(2)] AS FLOAT64) / 60
      + CAST(SPLIT(doorlooptijdTotEersteContactpoging, ':')[OFFSET(3)] AS FLOAT64) / 3600
    ) AS avg_doorlooptijd_hours,
    SAFE_DIVIDE(
      COUNT(DISTINCT IF(deadlineGehaald = 'true', LEFleadID, NULL)),
      COUNT(DISTINCT LEFleadID)
    ) AS deals_pct
  FROM ${ref("df_rawdata_views", "lef_leads")}
  GROUP BY week
)

SELECT
  AVG(avg_doorlooptijd_hours) AS mean_doorlooptijd_hours,
  STDDEV(avg_doorlooptijd_hours) AS std_doorlooptijd_hours,
  AVG(deals_pct) AS mean_deals,
  STDDEV(deals_pct) AS std_deals
FROM weekly_metrics
`
let refs = getRefs()
module.exports = {query, refs}
