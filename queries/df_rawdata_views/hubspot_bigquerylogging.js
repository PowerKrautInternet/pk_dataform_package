/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
    RECEIVEDON AS flow_date,
    JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
    JSON_VALUE(PAYLOAD, '$.hs_callback_id') AS hs_callback_id,
    JSON_VALUE(PAYLOAD, '$.type') AS type,
    JSON_VALUE(PAYLOAD, '$.data.callbackId') AS callback_id,
    JSON_VALUE(PAYLOAD, '$.data.origin.portalId') AS portal_id,
    JSON_VALUE(PAYLOAD, '$.data.origin.userId') AS user_id,
    JSON_VALUE(PAYLOAD, '$.data.origin.actionDefinitionId') AS action_definition_id,
    JSON_VALUE(PAYLOAD, '$.data.origin.actionDefinitionVersion') AS action_definition_version,
    JSON_VALUE(PAYLOAD, '$.data.origin.actionExecutionIndexIdentifier.enrollmentId') AS origin_enrollment_id,
    JSON_VALUE(PAYLOAD, '$.data.origin.actionExecutionIndexIdentifier.actionExecutionIndex') AS origin_action_execution_index,
    JSON_VALUE(PAYLOAD, '$.data.origin.extensionDefinitionVersionId') AS extension_definition_version_id,
    JSON_VALUE(PAYLOAD, '$.data.origin.extensionDefinitionId') AS extension_definition_id,
    JSON_VALUE(PAYLOAD, '$.data.context.workflowId') AS workflow_id,
    JSON_VALUE(PAYLOAD, '$.data.context.actionId') AS action_id,
    JSON_VALUE(PAYLOAD, '$.data.context.actionExecutionIndexIdentifier.enrollmentId') AS context_enrollment_id,
    JSON_VALUE(PAYLOAD, '$.data.context.actionExecutionIndexIdentifier.actionExecutionIndex') AS context_action_execution_index,
    JSON_VALUE(PAYLOAD, '$.data.context.source') AS source,
    JSON_VALUE(PAYLOAD, '$.data.object.objectId') AS object_id,
    JSON_VALUE(PAYLOAD, '$.data.object.objectType') AS object_type,
    JSON_VALUE(PAYLOAD, '$.data.fields.associate_contact') AS fields_associate_contact,
    JSON_VALUE(PAYLOAD, '$.data.inputFields.associate_contact') AS input_fields_associate_contact,
    'workflows' AS source_flow,
    workflow_name
FROM
    ${ref("rawdata","sendBigQueryMessageDataProducer")}
        
LEFT JOIN
    ${ref("googleSheets","gs_workflow_mapping")}
    ON workflow_id = JSON_VALUE(PAYLOAD, '$.data.context.workflowId')

`
let refs = pk.getRefs()
module.exports = {query, refs}
