/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        'Microsoft Ads' AS bron,
        IFNULL(adgroup.account, pf.account) AS account,
        IFNULL(adgroup.pk_crm_id, pf.pk_crm_id) AS pk_crm_id,
        IFNULL(account_id, account_id) AS account_id,
        IFNULL(campaign_id, campaign_id) AS campaign_id,
        IFNULL(adgroup_id, asset_group_id) AS adgroup_id,
        device_type,
        IFNULL(time_period, time_period) AS time_period,
        MAX(IFNULL(account_name, account_name)) AS account_name,
        MAX(account_number) AS account_number,
        MAX(ad_distribution) AS ad_distribution,
        MAX(IFNULL(account_status, account_status)) AS account_status,
        MAX(IFNULL(campaign_name, campaign_name)) AS campaign_name,
        MAX(IFNULL(campaign_status, campaign_status)) AS campaign_status,
        MAX(IFNULL(campaign_type, "Performance Max")) AS campaign_type,
        MAX(customer_id) AS customer_id,
        MAX(customer_name) AS customer_name,
        MAX(IFNULL(adgroup_name, asset_group_name)) AS adgroup_name,
        MAX(IFNULL(adgroup_type, "Performance Max")) AS adgroup_type,
        MAX(adgroup_labels) AS adgroup_labels,
        MAX(IFNULL(adgroup_status, asset_group_status)) AS adgroup_status,
        SUM(CAST(IFNULL(adgroup.conversions, pf.conversions) AS INT64)) AS conversions,
        SUM(CAST(conversions_qualified AS FLOAT64)) AS conversions_qualified,
        SUM(CAST(all_conversions AS INT64)) AS all_conversions,
        SUM(CAST(IFNULL(adgroup.impressions, pf.impressions) AS INT64)) AS impressions,
        SUM(CAST(IFNULL(adgroup.clicks, pf.clicks) AS INT64)) AS clicks,
        SUM(CAST(IFNULL(adgroup.spend, pf.spend) AS FLOAT64)) AS spend,
        MAX(quality_score) AS quality_score

    FROM ${ref("df_rawdata_views", "bing_ad_group_performance")} adgroup
    
    FULL OUTER JOIN ${ref("df_rawdata_views", "bing_assetgroup_performance")} pf
    ON 1=0
    
    GROUP BY
        account,
        pk_crm_id,
        account_id,
        campaign_id,
        adgroup_id,
        device_type,
        time_period

`
let refs = pk.getRefs()
module.exports = {query, refs}
