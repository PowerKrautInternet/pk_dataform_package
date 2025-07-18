/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        account,
        JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
        JSON_VALUE(PAYLOAD, '$.type') AS type,
        JSON_VALUE(PAYLOAD, '$.id') AS id,
        JSON_VALUE(PAYLOAD, '$.response.test') AS test,
        JSON_VALUE(PAYLOAD, '$.response.notifiedOnCreativeRejection') AS notifiedOnCreativeRejection,
        JSON_VALUE(PAYLOAD, '$.response.notifiedOnNewFeaturesEnabled') AS notifiedOnNewFeaturesEnabled,
        JSON_VALUE(PAYLOAD, '$.response.notifiedOnEndOfCampaign') AS notifiedOnEndOfCampaign,
        JSON_VALUE(PAYLOAD, '$.response.servingStatuses[0]') AS servingStatus,
        JSON_VALUE(PAYLOAD, '$.response.notifiedOnCampaignOptimization') AS notifiedOnCampaignOptimization,
        JSON_VALUE(PAYLOAD, '$.response.type') AS response_type,
        JSON_VALUE(PAYLOAD, '$.response.version.versionTag') AS versionTag,
        JSON_VALUE(PAYLOAD, '$.response.reference') AS reference,
        JSON_VALUE(PAYLOAD, '$.response.notifiedOnCreativeApproval') AS notifiedOnCreativeApproval,
        JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.created.actor') AS created_actor,
        JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.created.time') AS created_time,
        JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.lastModified.actor') AS lastModified_actor,
        JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.lastModified.time') AS lastModified_time,
        JSON_VALUE(PAYLOAD, '$.response.name') AS name,
        JSON_VALUE(PAYLOAD, '$.response.currency') AS currency,
        JSON_VALUE(PAYLOAD, '$.response.status') AS status
    FROM
        ${ref("linkedInAdsDataProducer_lasttransaction")}
    WHERE
        JSON_VALUE(PAYLOAD, '$.type') = "AdAccountInformationPublisher"
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
