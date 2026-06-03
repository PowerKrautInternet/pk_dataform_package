/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


SELECT 
datum_bericht,
verstuurd_bericht,
email,
kenteken,
werkplaats_vestiging,
voertuig_id
FROM(
  SELECT
    CAST(TIMESTAMP_MILLIS(CAST(datum_bericht AS INT64)) AS DATE) AS datum_bericht,
    flows.workflow_name AS verstuurd_bericht,
    berijder_email AS email,
    kenteken,
    werkplaats_vestiging,
    workflow_id,
    voertuig_id
  FROM(
    SELECT 
    *
    FROM
    ${ref("df_staging_views","stg_hubspotworkflows_voertuigen")} 
  ) msg

  LEFT JOIN
    ${ref("googleSheets","gs_workflow_mapping")} flows
  ON
    msg.workflow_id = flows.workflow_id
  AND
    flows.type = "Aftersales"
  WHERE workflow_id <> "" AND workflow_id is not null
) voertuig
`
let refs = pk.getRefs()
module.exports = {query, refs}
