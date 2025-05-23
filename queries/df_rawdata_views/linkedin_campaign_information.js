/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
        JSON_VALUE(PAYLOAD, '$.type') AS type,
        JSON_VALUE(PAYLOAD, '$.id') AS id,
        JSON_VALUE(PAYLOAD, '$.account') AS account,
        JSON_VALUE(PAYLOAD, '$.response.totalBudget.currencyCode') AS totalBudget_currencyCode,
        JSON_VALUE(PAYLOAD, '$.response.totalBudget.amount') AS totalBudget_amount,
        JSON_VALUE(PAYLOAD, '$.response.storyDeliveryEnabled') AS storyDeliveryEnabled,
        JSON_VALUE(PAYLOAD, '$.response.pacingStrategy') AS pacingStrategy,
        JSON_VALUE(PAYLOAD, '$.response.locale.country') AS locale_country,
        JSON_VALUE(PAYLOAD, '$.response.locale.language') AS locale_language,
        JSON_VALUE(PAYLOAD, '$.response.type') AS response_type,
        JSON_VALUE(PAYLOAD, '$.response.runSchedule.start') AS runSchedule_start,
        JSON_VALUE(PAYLOAD, '$.response.runSchedule.end') AS runSchedule_end,
        JSON_VALUE(PAYLOAD, '$.response.optimizationTargetType') AS optimizationTargetType,
        JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.created.time') AS created_time,
        JSON_VALUE(PAYLOAD, '$.response.changeAuditStamps.lastModified.time') AS lastModified_time,
        JSON_VALUE(PAYLOAD, '$.response.costType') AS costType,
        JSON_VALUE(PAYLOAD, '$.response.creativeSelection') AS creativeSelection,
        JSON_VALUE(PAYLOAD, '$.response.offsiteDeliveryEnabled') AS offsiteDeliveryEnabled,
        JSON_VALUE(PAYLOAD, '$.response.audienceExpansionEnabled') AS audienceExpansionEnabled,
        JSON_VALUE(PAYLOAD, '$.response.test') AS test,
        JSON_VALUE(PAYLOAD, '$.response.format') AS format,
        JSON_VALUE(PAYLOAD, '$.response.servingStatuses[0]') AS servingStatus_0,
        JSON_VALUE(PAYLOAD, '$.response.servingStatuses[1]') AS servingStatus_1,
        JSON_VALUE(PAYLOAD, '$.response.servingStatuses[2]') AS servingStatus_2,
        JSON_VALUE(PAYLOAD, '$.response.version.versionTag') AS versionTag,
        JSON_VALUE(PAYLOAD, '$.response.objectiveType') AS objectiveType,
        JSON_VALUE(PAYLOAD, '$.response.associatedEntity') AS associatedEntity,
        JSON_VALUE(PAYLOAD, '$.response.campaignGroup') AS campaignGroup,
        JSON_VALUE(PAYLOAD, '$.response.unitCost.currencyCode') AS unitCost_currencyCode,
        JSON_VALUE(PAYLOAD, '$.response.unitCost.amount') AS unitCost_amount,
        JSON_VALUE(PAYLOAD, '$.response.name') AS name,
        JSON_VALUE(PAYLOAD, '$.response.status') AS status
    FROM
        ${ref("linkedInAdsDataProducer_lasttransaction")}
    WHERE
        JSON_VALUE(PAYLOAD, '$.type') = "CampaignInformationPublisher"
    
`
let refs = pk.getRefs()
module.exports = {query, refs}