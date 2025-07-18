/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT
    account,
    JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
    JSON_VALUE(PAYLOAD, '$.type') AS type,
    JSON_VALUE(PAYLOAD, '$.accountId') AS accountId,
    JSON_VALUE(PAYLOAD, '$.sponsoredAccount') AS sponsoredAccount,
    JSON_VALUE(PAYLOAD, '$.sponsoredCampaignGroup') AS sponsoredCampaignGroup,
    JSON_VALUE(PAYLOAD, '$.sponsoredCampaign') AS sponsoredCampaign,
    JSON_VALUE(PAYLOAD, '$.sponsoredCreative') AS sponsoredCreative,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(PAYLOAD, '$.startDate')) AS startDate,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(PAYLOAD, '$.endDate')) AS endDate,
    JSON_VALUE(PAYLOAD, '$.response.comments') AS comments,
    JSON_VALUE(PAYLOAD, '$.response.costInUsd') AS costInUsd,
    JSON_VALUE(PAYLOAD, '$.response.landingPageClicks') AS landingPageClicks,
    JSON_VALUE(PAYLOAD, '$.response.adUnitClicks') AS adUnitClicks,
    JSON_VALUE(PAYLOAD, '$.response.companyPageClicks') AS companyPageClicks,
    JSON_VALUE(PAYLOAD, '$.response.costInLocalCurrency') AS costInLocalCurrency,
    JSON_VALUE(PAYLOAD, '$.response.impressions') AS impressions,
    JSON_VALUE(PAYLOAD, '$.response.otherEngagements') AS otherEngagements,
    JSON_VALUE(PAYLOAD, '$.response.shares') AS shares,
    JSON_VALUE(PAYLOAD, '$.response.externalWebsiteConversions') AS externalWebsiteConversions,
    JSON_VALUE(PAYLOAD, '$.response.clicks') AS clicks,
    JSON_VALUE(PAYLOAD, '$.response.totalEngagements') AS totalEngagements,
    JSON_VALUE(PAYLOAD, '$.response.reactions') AS reactions,
    JSON_VALUE(PAYLOAD, '$.response.likes') AS likes,
    JSON_VALUE(PAYLOAD, '$.response.downloadClicks') AS downloadClicks
FROM
    ${ref("linkedInAdsDataProducer_lasttransaction")}
WHERE
    JSON_VALUE(PAYLOAD, '$.type') = "AdsAnalyticsPublisher"
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
