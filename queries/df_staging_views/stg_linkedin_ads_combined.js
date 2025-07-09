/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
    
    SELECT 
    "LinkedIn" AS bron,
    MAX(ads_analytics.account) AS account,
    ads_analytics.pk_crm_id,
    ads_analytics.accountId,
    TRIM(REPLACE(ads_analytics.sponsoredCampaign, "urn:li:sponsoredCampaign:", "")) as campaign_id,
    TRIM(REPLACE(ads_analytics.sponsoredCampaignGroup, "urn:li:sponsoredCampaignGroup:", "")) as campaign_group_id,
    CAST(ads_analytics.startDate AS DATE) AS startDate,
    MAX(CAST(ads_analytics.endDate AS DATE)) AS endDate,
    SUM(CAST(ads_analytics.comments AS INT64)) AS comments,
    SUM(CAST(ads_analytics.landingPageClicks AS INT64)) AS interactions,
    SUM(CAST(ads_analytics.adUnitClicks AS INT64)) AS ad_unit_clicks,
    SUM(CAST(ads_analytics.companyPageClicks AS INT64)) AS company_page_clicks,
    SUM(CAST(ads_analytics.costInLocalCurrency AS FLOAT64)) AS cost,
    SUM(CAST(ads_analytics.impressions AS INT64)) AS impressions,
    SUM(CAST(ads_analytics.otherEngagements AS INT64)) AS other_engagements,
    SUM(CAST(ads_analytics.shares AS INT64)) AS shares,
    SUM(CAST(ads_analytics.externalWebsiteConversions AS INT64)) AS website_conversions,
    SUM(CAST(ads_analytics.clicks AS INT64)) AS clicks,
    SUM(CAST(ads_analytics.totalEngagements AS INT64)) AS engagements,
    SUM(CAST(ads_analytics.reactions AS INT64)) AS reactions,
    SUM(CAST(ads_analytics.likes AS INT64)) AS likes,
    SUM(CAST(ads_analytics.downloadClicks AS INT64)) AS download_clicks,
    
    MAX(ad_account.name) as account_name,
    MAX(ad_account.status) as account_status,
    MAX(ad_account.currency) as account_currency,
    
    MAX(campaign_information.name) as campaign_name,
    MAX(campaign_information.status) as campaign_status,
    MAX(campaign_information.totalBudget_amount) as campaign_budget,
    MAX(campaign_information.optimizationTargetType) as campaign_optimization_target_type,
    MAX(campaign_information.costType) as campaign_cost_type,
    MAX(campaign_information.objectiveType) as campaign_objective_type,
    
    MAX(ad_campaign_group.name) as campaign_group_name,
    MAX(ad_campaign_group.status) as campaign_group_status,
    MAX(ad_campaign_group.totalBudget_amount) as campaign_group_budget
    
    FROM ${ref('linkedin_ads_analytics')} ads_analytics
    
    LEFT JOIN ${ref("linkedin_ad_account")} ad_account
    ON TRIM(ads_analytics.accountId) = TRIM(ad_account.id) AND ads_analytics.account = ad_account.account
    
    LEFT JOIN ${ref("linkedin_campaign_information")} campaign_information
    ON TRIM(REPLACE(ads_analytics.sponsoredCampaign, "urn:li:sponsoredCampaign:", "")) = TRIM(campaign_information.id) AND ads_analytics.account = campaign_information.account
    
    LEFT JOIN ${ref("linkedin_ad_campaign_group")} ad_campaign_group
    ON TRIM(REPLACE(ads_analytics.sponsoredCampaignGroup, "urn:li:sponsoredCampaignGroup:", "")) = TRIM(ad_campaign_group.id) AND ads_analytics.account = ad_campaign_group.account
    
    GROUP BY
    pk_crm_id,
    accountId,
    campaign_id,
    campaign_group_id,
    startDate

    `
let refs = pk.getRefs()
module.exports = {query, refs}
