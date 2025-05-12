/*config*/
let pk = require("../../setup")
let ref = pk.ref
let query = `

SELECT 
*,
    ${ref("lookupTable")}(
        campaign_name, 
        TO_JSON_STRING(ARRAY(SELECT merk FROM ${ref("GS_MERKEN")}))
    ) as merk,

FROM(
    SELECT
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.bron, facebook.bron), dv360.bron), microsoft.bron), linkedin.bron) as bron,
    IFNULL(IFNULL(IFNULL(IFNULL(CAST(google_ads.account_id AS STRING), facebook.account_id), CAST(dv360.advertiser_id AS STRING)), microsoft.account_id), linkedin.accountId) as account_id,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.account_name, facebook.account_name), dv360.advertiser), microsoft.account_name), linkedin.account_name) as account_name,
    IFNULL(IFNULL(IFNULL(IFNULL(CAST(google_ads.campaign_id AS STRING), facebook.campaign_id), CAST(dv360.insertion_order_id AS STRING)), microsoft.campaign_id), linkedin.campaign_id) as campaign_id,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.campaign_name, facebook.campaign_name), dv360.insertion_order), microsoft.campaign_name), linkedin.campaign_name) as campaign_name,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.segments_date, facebook.date_start), dv360.date), CAST(microsoft.time_period AS DATE)), linkedin.startDate)  as record_date,
    IFNULL(IFNULL(IFNULL(IFNULL(CAST(google_ads.ad_group_id AS STRING), facebook.adset_id), CAST(dv360.line_item_id AS STRING)), microsoft.adgroup_id), linkedin.campaign_group_id)  as ad_group_id,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.ad_group_name, facebook.adset_name), dv360.line_item), microsoft.adgroup_name), linkedin.campaign_group_name) as ad_group_name,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.impressions, facebook.impressions), dv360.impressions), microsoft.impressions), linkedin.impressions) as ads_impressions,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.interactions, facebook.link_click), dv360.clicks), microsoft.clicks), linkedin.interactions) as ads_interactions,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.conversions, facebook.lead), dv360.click_through_conversions), microsoft.all_conversions), linkedin.website_conversions) as ads_conversions,
    IFNULL(IFNULL(IFNULL(IFNULL(google_ads.Cost, facebook.spend), dv360.revenue_advertiser_currency), microsoft.spend), linkedin.cost) as ads_cost,

    --IFNULL(IFNULL(google_ads.campaign_start_date, CAST(facebook.date_start AS DATE)), linkedin.startDate)  as campaign_start_date,
    --IFNULL(IFNULL(google_ads.campaign_end_date, CAST(facebook.date_stop AS DATE)), linkedin.endDate) as campaign_end_date,
    IFNULL(IFNULL(google_ads.campaign_advertising_channel_type, microsoft.campaign_type), linkedin.campaign_cost_type) AS campaign_advertising_channel_type,
    IFNULL(IFNULL(google_ads.campaign_bidding_strategy_type, line_item_type), linkedin.campaign_optimization_target_type) AS bidding_strategy_type,
    IFNULL(IFNULL(google_ads.campaign_status, microsoft.campaign_status), linkedin.campaign_status) AS campaign_status,
    IFNULL(IFNULL(google_ads.ad_group_status, dv360.insertion_order_status), linkedin.campaign_group_status) AS ad_group_status,
    IFNULL(google_ads.ad_group_type, microsoft.adgroup_type) AS ad_group_type,
    google_ads.conversions_value,
    facebook.ad_id AS facebook_ad_id,
    facebook.ad_name AS facebook_ad_name,
    IFNULL(facebook.objective, linkedin.campaign_objective_type) AS objective,
    dv360.creative AS dv360_creative,
    dv360.creative_size AS dv360_creative_size,
    dv360.creative_type AS dv360_creative_type,
    dv360.site AS dv360_site,
    dv360.rich_media_video_plays AS dv360_rich_media_video_plays,
    --dv360.rich_media_video_completions AS dv360_rich_media_video_completions,
    dv360.rich_media_video_pauses AS dv360_rich_media_video_pauses,
    dv360.rich_media_video_skips AS dv360_rich_media_video_skips,
    dv360.rich_media_video_mutes AS dv360_rich_media_video_mutes,
    dv360.rich_media_video_first_quartile_completes AS dv360_rich_media_video_first_quartile_completes,
    dv360.rich_media_video_midpoints AS dv360_rich_media_video_midpoints,
    dv360.rich_media_video_third_quartile_completes AS dv360_rich_media_video_third_quartile_completes,
    dv360.rich_media_audio_plays AS dv360_rich_media_audio_plays,
    dv360.rich_media_audio_pauses AS dv360_rich_media_audio_pauses,
    dv360.rich_media_audio_stops AS dv360_rich_media_audio_stops,
    dv360.rich_media_audio_mutes AS dv360_rich_media_audio_mutes,
    dv360.rich_media_audio_first_quartile_completes AS dv360_rich_media_audio_first_quartile_completes,
    dv360.rich_media_audio_midpoints AS dv360_rich_media_audio_midpoints,
    dv360.rich_media_audio_third_quartile_completes AS dv360_rich_media_audio_third_quartile_completes,
    dv360.rich_media_audio_completions AS dv360_rich_media_audio_completions,
    --dv360.media_fee1_advertiser_currency AS dv360_media_fee1_advertiser_currency,
    --dv360.media_fee2_advertiser_currency AS dv360_media_fee2_advertiser_currency,
    --dv360.media_fee3_advertiser_currency AS dv360_media_fee3_advertiser_currency,
    microsoft.quality_score,
    linkedin.comments,
    linkedin.shares,
    linkedin.reactions,
    linkedin.likes,
    linkedin.download_clicks

FROM ${ref("df_staging_views", "stg_google_ads_adgroup_combined")} google_ads

FULL OUTER JOIN ${ref("df_staging_views", "stg_facebookdata")} facebook
ON 1=0

FULL OUTER JOIN ${ref("df_rawdata_views", "dv360_data")} dv360
ON 1=0

FULL OUTER JOIN ${ref("df_staging_views", "stg_bing_ad_group_performance")} microsoft
ON 1=0

FULL OUTER JOIN ${ref("df_staging_views", "stg_linkedin_ads_combined")} linkedin 
ON 1=0)
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}