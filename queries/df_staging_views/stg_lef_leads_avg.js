/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
WITH weekly_metrics AS (
  SELECT
    medewerker,
    vestiging,
    EXTRACT(WEEK FROM aangemaaktDatum) AS week,
    COUNT(DISTINCT LEFleadID) AS leads_count,
  FROM ${ref("df_rawdata_views", "lef_leads")}
  GROUP BY medewerker, vestiging, week
)

SELECT
  medewerker,
  vestiging,
  week,
  AVG(leads_count) OVER (PARTITION BY medewerker, vestiging) AS mean_leads,
  STDDEV(leads_count) OVER (PARTITION BY medewerker, vestiging) AS std_leads,
FROM weekly_metrics) mean_stddev_leads
ON mean_stddev_leads.medewerker = lef.medewerker
AND mean_stddev_leads.vestiging = lef.vestiging
AND mean_stddev_leads.week = EXTRACT(WEEK FROM lef.aangemaaktDatum
`
let refs = getRefs()
module.exports = {query, refs}
