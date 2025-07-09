/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        'Microsoft Ads' as bron,
        IFNULL(adgroup.pk_crm_id, pf.pk_crm_id) AS pk_crm_id,
        IFNULL(account_id, AccountId) AS account_id,
        IFNULL(campaign_id, CampaignId) AS campaign_id,
        IFNULL(adgroup_id, AssetGroupId) AS adgroup_id,
        device_type,
        IFNULL(time_period, TimePeriod) AS time_period,
        MAX(IFNULL(account_name, AccountName)) AS account_name,
        MAX(account_number) AS account_number,
        MAX(ad_distribution) AS ad_distribution,
        MAX(IFNULL(account_status, AccountStatus)) AS account_status,
        MAX(IFNULL(campaign_name, CampaignName)) AS campaign_name,
        MAX(IFNULL(campaign_status, CampaignStatus)) AS campaign_status,
        MAX(IFNULL(campaign_type, "Performance Max")) AS campaign_type,
        MAX(customer_id) AS customer_id,
        MAX(customer_name) AS customer_name,
        MAX(IFNULL(adgroup_name, AssetGroupName)) AS adgroup_name,
        MAX(IFNULL(adgroup_type, "Performance Max")) AS adgroup_type,
        MAX(adgroup_labels) AS adgroup_labels,
        MAX(IFNULL(adgroup_status, AssetGroupStatus)) AS adgroup_status,
        SUM(CAST(IFNULL(adgroup.conversions, pf.conversions) AS INT64)) AS conversions,
        SUM(CAST(conversions_qualified AS FLOAT64)) AS conversions_qualified,
        SUM(CAST(all_conversions AS INT64)) AS all_conversions,
        SUM(CAST(IFNULL(adgroup.impressions, pf.impressions) AS INT64)) AS impressions,
        SUM(CAST(IFNULL(adgroup.clicks, pf.clicks) AS INT64)) AS clicks,
        SUM(CAST(IFNULL(adgroup.spend, pf.spend) AS FLOAT64)) AS spend,
        MAX(quality_score) AS quality_score
    
    FROM ${ref("df_rawdata_views", "bing_ad_group_performance")}
    
    FULL OUTER JOIN ${ref("df_rawdata_views", "bing_assetgroup_performance")} pf
    ON 1=0
             LEFT JOIN
         (SELECT
              session_source_medium,
              event_date,
              SUM(conversion_event) AS conversions_total,
              session_campaign,
              session_content
          FROM
              ${ref("df_staging_views", "stg_ga4_mappings_targets")}
          WHERE session_source_medium = "bing / cpc"
          GROUP BY session_source_medium,
                   event_date,
                   session_campaign,
                   session_content
         )
         ON event_date = DATE(time_period)
             AND session_campaign = campaign_name
             AND session_content = adgroup_name
    GROUP BY
        pk_crm_id,
        account_id,
        campaign_id,
        adgroup_id,
        time_period

`
let refs = pk.getRefs()
module.exports = {query, refs}
