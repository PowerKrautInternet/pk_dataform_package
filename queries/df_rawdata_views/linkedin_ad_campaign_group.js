/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
    account,
    JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
    JSON_VALUE(PAYLOAD, '$.type') AS type,
    JSON_VALUE(PAYLOAD, '$.id') AS id,
    JSON_VALUE(PAYLOAD, '$.account') AS account_id,
    JSON_VALUE(PAYLOAD, '$.response.runSchedule.start') AS runSchedule_start,
    JSON_VALUE(PAYLOAD, '$.response.runSchedule.end') AS runSchedule_end,
    JSON_VALUE(PAYLOAD, '$.response.test') AS test,
    JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.created.time') AS created_time,
    JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.lastModified.time') AS lastModified_time,
    JSON_VALUE(PAYLOAD, '$.response.totalBudget.currencyCode') AS totalBudget_currencyCode,
    JSON_VALUE(PAYLOAD, '$.response.totalBudget.amount') AS totalBudget_amount,
    JSON_VALUE(PAYLOAD, '$.response.name') AS name,
    JSON_VALUE(PAYLOAD, '$.response.servingStatuses[0]') AS servingStatus,
    JSON_VALUE(PAYLOAD, '$.response.backfilled') AS backfilled,
    JSON_VALUE(PAYLOAD, '$.response.status') AS status
FROM
  ${ref("linkedInAdsDataProducer_lasttransaction")}
WHERE
  JSON_VALUE(PAYLOAD, '$.type') = "AdCampaignGroupInformationPublisher"
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
