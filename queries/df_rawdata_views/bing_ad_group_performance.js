/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        account,
        JSON_VALUE(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
        JSON_VALUE(PAYLOAD, '$.type') AS type,
        JSON_VALUE(PAYLOAD, '$.AccountId') AS account_id,
        JSON_VALUE(PAYLOAD, '$.AccountName') AS account_name,
        JSON_VALUE(PAYLOAD, '$.AccountNumber') AS account_number,
        JSON_VALUE(PAYLOAD, '$.CampaignId') AS campaign_id,
        JSON_VALUE(PAYLOAD, '$.CampaignName') AS campaign_name,
        JSON_VALUE(PAYLOAD, '$.TimePeriod') AS time_period,
        JSON_VALUE(PAYLOAD, '$.AdDistribution') AS ad_distribution,
        JSON_VALUE(PAYLOAD, '$.AdGroupId') AS adgroup_id,
        JSON_VALUE(PAYLOAD, '$.DeviceType') AS device_type,
        JSON_VALUE(PAYLOAD, '$.response.AccountStatus') AS account_status,
        JSON_VALUE(PAYLOAD, '$.response.CampaignStatus') AS campaign_status,
        JSON_VALUE(PAYLOAD, '$.response.CampaignType') AS campaign_type,
        JSON_VALUE(PAYLOAD, '$.response.CustomerName') AS customer_name,
        JSON_VALUE(PAYLOAD, '$.response.CustomerId') AS customer_id,
        JSON_VALUE(PAYLOAD, '$.response.AdGroupType') AS adgroup_type,
        JSON_VALUE(PAYLOAD, '$.response.AdGroupLabels') AS adgroup_labels,
        JSON_VALUE(PAYLOAD, '$.response.Status') AS adgroup_status,
        JSON_VALUE(PAYLOAD, '$.response.AdGroupName') AS adgroup_name,
        JSON_VALUE(PAYLOAD, '$.response.Conversions') AS conversions,
        JSON_VALUE(PAYLOAD, '$.response.ConversionsRate') AS conversions_rate,
        JSON_VALUE(PAYLOAD, '$.response.ConversionsQualified') AS conversions_qualified,
        JSON_VALUE(PAYLOAD, '$.response.CostPerConversion') AS cost_per_conversion,
        JSON_VALUE(PAYLOAD, '$.response.AllConversions') AS all_conversions,
        JSON_VALUE(PAYLOAD, '$.response.AllConversionRate') AS all_conversions_rate,
        JSON_VALUE(PAYLOAD, '$.response.AllConversionsQualified') AS all_conversions_rate_qualified,
        JSON_VALUE(PAYLOAD, '$.response.AllCostPerConversion') AS all_cost_per_conversion,
        JSON_VALUE(PAYLOAD, '$.response.ViewThroughConversions') AS view_through_conversions,
        JSON_VALUE(PAYLOAD, '$.response.ViewThroughRate') AS view_through_rate,
        JSON_VALUE(PAYLOAD, '$.response.Assists') AS assists,
        JSON_VALUE(PAYLOAD, '$.response.AudienceImpressionSharePercent') AS audience_impressionshare_percent,
        JSON_VALUE(PAYLOAD, '$.response.AudienceImpressionLostToRankPercent') AS audience_impressionlosttorank_percent,
        JSON_VALUE(PAYLOAD, '$.response.AudienceImpressionLostToBudgetPercent') AS audience_impressionlosttobudget_percent,
        JSON_VALUE(PAYLOAD, '$.response.AverageCpm') AS average_cpm,
        JSON_VALUE(PAYLOAD, '$.response.AverageCpc') AS average_cpc,
        JSON_VALUE(PAYLOAD, '$.response.Impressions') AS impressions,
        JSON_VALUE(PAYLOAD, '$.response.Clicks') AS clicks,
        JSON_VALUE(PAYLOAD, '$.response.Ctr') AS ctr,
        JSON_VALUE(PAYLOAD, '$.response.RelativeCtr') AS relative_ctr,
        JSON_VALUE(PAYLOAD, '$.response.QualityScore') AS quality_score,
        JSON_VALUE(PAYLOAD, '$.response.Spend') AS spend,
        JSON_VALUE(PAYLOAD, '$.response.BaseCampaignId') AS base_campaign_id

    FROM ${ref("bingAdsDataProducer_lasttransaction")}
    WHERE JSON_VALUE(PAYLOAD, '$.type') = 'AdGroupPerformanceReportPublisher'
    
`
let refs = pk.getRefs()
module.exports = {query, refs}
