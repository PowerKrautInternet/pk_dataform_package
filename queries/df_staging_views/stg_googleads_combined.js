/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
    
SELECT 
    IFNULL(adgroup_stats.bron, perf_max_stats.bron) as bron,
    IFNULL(adgroup_stats.customer_id, perf_max_stats.customer_id) as account_id,
    --IFNULL(adgroup_stats.account, perf_max_stats.account) as account,
    IFNULL(adgroup_stats.campaign_id, perf_max_stats.campaign_id) as campaign_id,
    IFNULL(adgroup_stats.campaign_name, perf_max_stats.campaign_name) as campaign_name,
    IFNULL(adgroup_stats.campaign_advertising_channel_type, perf_max_stats.campaign_advertising_channel_type) as campaign_advertising_channel_type,
    IFNULL(adgroup_stats.campaign_bidding_strategy_type, perf_max_stats.campaign_bidding_strategy_type) as campaign_bidding_strategy_type,
    IFNULL(adgroup_stats.campaign_start_date, perf_max_stats.campaign_start_date) as campaign_start_date,
    IFNULL(adgroup_stats.campaign_end_date, perf_max_stats.campaign_end_date) as campaign_end_date,
    IFNULL(adgroup_stats.campaign_status, perf_max_stats.campaign_status) as campaign_status,
    IFNULL(adgroup_stats.segments_date, perf_max_stats.segments_date) as segments_date,
    ad_group_id,
    ad_group_name,
    ad_group_status,
    ad_group_type,
    ad_group_bidding_strategy_type,
    IFNULL(adgroup_stats.segments_conversion_action_name, perf_max_stats.segments_conversion_action_name) as conversion_action_name,
    IFNULL(adgroup_stats.impressions, perf_max_stats.impressions) as impressions,
    IFNULL(adgroup_stats.interactions, perf_max_stats.interactions) as interactions,
    IFNULL(adgroup_stats.clicks, perf_max_stats.clicks) as clicks,
    IFNULL(adgroup_stats.Cost, perf_max_stats.Cost) as cost,
    IFNULL(adgroup_stats.conversions, perf_max_stats.conversions) as conversions,
    IFNULL(adgroup_stats.conversions_value, perf_max_stats.conversions_value) as conversions_value,
    ${ifSource("googleads_campaignlabel","label.merk AS merk,")}
    ${ifSource("googleads_campaignlabel","label.model AS model,")}
    ${ifSource("googleads_campaignlabel","label.campagnegroep AS campagnegroep,")}

FROM ${ref('df_staging_views', 'stg_google_ads_adgroup_combined')} adgroup_stats

FULL OUTER JOIN ${ref('df_staging_views', 'stg_googleads_perfmax_combined')} perf_max_stats
ON 1=0

${join("LEFT JOIN", "df_rawdata_views", "googleads_campaignlabel", "AS label ON IFNULL(adgroup_stats.campaign_id, perf_max_stats.campaign_id) = label.campaign_id")}
    
`
let refs = getRefs()
module.exports = {query, refs}
