/*config*/
let pk = require("../../setup")
let ref = pk.ref
let query = `

    SELECT
        'Microsoft Ads' as bron,
        pk_crm_id,
        account_id,
        campaign_id,
        adgroup_id,
        time_period,
        MAX(account_name) AS account_name,
        MAX(account_number) AS account_number,
        MAX(ad_distribution) AS ad_distribution,
        MAX(account_status) AS account_status,
        MAX(campaign_name) AS campaign_name,
        MAX(campaign_status) AS campaign_status,
        MAX(campaign_type) AS campaign_type,
        MAX(customer_id) AS customer_id,
        MAX(customer_name) AS customer_name,
        MAX(adgroup_name) AS adgroup_name,
        MAX(adgroup_type) AS adgroup_type,
        MAX(adgroup_labels) AS adgroup_labels,
        MAX(adgroup_status) AS adgroup_status,
        SUM(CAST(conversions AS INT64)) AS conversions,
        SUM(CAST(conversions_qualified AS FLOAT64)) AS conversions_qualified,
        SUM(conversions_total) AS all_conversions,
        SUM(CAST(impressions AS INT64)) AS impressions,
        SUM(CAST(clicks AS INT64)) AS clicks,
        SUM(CAST(spend AS FLOAT64)) AS spend,
        MAX(quality_score) AS quality_score,


    FROM ${ref("bing_ad_group_performance")}

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