/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `

    SELECT
        'Google Ads' as bron,
        campaign.customer_id,
        campaign.campaign_id,
        MAX(campaign.account) AS account_name,
        MAX(campaign.campaign_name) AS campaign_name,
        MAX(campaign.campaign_advertising_channel_type) AS campaign_advertising_channel_type,
        MAX(campaign.campaign_bidding_strategy_type) AS campaign_bidding_strategy_type,
        MAX(campaign.campaign_start_date) AS campaign_start_date,
        MAX(campaign.campaign_end_date) AS campaign_end_date,
        MAX(campaign.campaign_status) AS campaign_status,
        campaign_stats.segments_date AS segments_date,
        SUM(campaign_stats.metrics_impressions) AS impressions,
        SUM(campaign_stats.metrics_interactions) AS interactions,
        SUM(campaign_stats.metrics_clicks) AS clicks,
        SUM(campaign_stats.metrics_conversions) AS conversions,
        SUM(campaign_stats.metrics_conversions_value) AS conversions_value,
        (SUM(campaign_stats.metrics_cost_micros) / 1000000) AS Cost,

    FROM ${ref('ads_Campaign')} campaign

             LEFT JOIN ${ref('ads_CampaignBasicStats')} campaign_stats
                       ON campaign.customer_id = campaign_stats.customer_id
                           AND campaign.campaign_id = campaign_stats.campaign_id

    WHERE
        campaign._DATA_DATE = campaign._LATEST_DATE
      AND campaign.campaign_advertising_channel_type = 'PERFORMANCE_MAX'
      AND campaign_stats.segments_date IS NOT NULL

    GROUP BY
        customer_id,
        campaign_id,
        segments_date



`
let refs = getRefs()
module.exports = {query, refs}
