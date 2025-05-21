/*config*/
let {ref, getRefs, join, ifNull, ifSource} = require("../../sources")
let query = `

SELECT 
    *,
    ${ref("lookupTable")}(campaign_name, TO_JSON_STRING(ARRAY(SELECT merk FROM ${ref("GS_MERKEN")}))) as merk,

FROM (
    SELECT
        ${ifNull(['google_ads.bron', ifSource('stg_facebookdata','facebook.bron'), ifSource('dv360_data','dv360.bron'), ifSource('stg_bing_ad_group_performance','microsoft.bron'), ifSource('stg_linkedin_ads_combined','linkedin.bron')])} as bron,
        ${ifNull(['CAST(google_ads.account_id AS STRING)', ifSource('stg_facebookdata','facebook.account_id'), ifSource('dv360_data','CAST(dv360.advertiser_id AS STRING)'), ifSource('stg_bing_ad_group_performance','microsoft.account_id'), ifSource('stg_linkedin_ads_combined','linkedin.accountId')])} as account_id,
        ${ifNull(['google_ads.account_name', ifSource('stg_facebookdata','facebook.account_name'), ifSource('dv360_data','dv360.advertiser'), ifSource('stg_bing_ad_group_performance','microsoft.account_name'), ifSource('stg_linkedin_ads_combined','linkedin.account_name')])} as account_name,
        ${ifNull(['CAST(google_ads.campaign_id AS STRING)', ifSource('stg_facebookdata','facebook.campaign_id'), ifSource('dv360_data','CAST(dv360.insertion_order_id AS STRING)'), ifSource('stg_bing_ad_group_performance','microsoft.campaign_id'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_id')])} as campaign_id,
        ${ifNull(['google_ads.campaign_name', ifSource('stg_facebookdata','facebook.campaign_name'), ifSource('dv360_data','dv360.insertion_order'), ifSource('stg_bing_ad_group_performance','microsoft.campaign_name'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_name')])} as campaign_name,
        ${ifNull(['google_ads.segments_date', ifSource('stg_facebookdata','facebook.date_start'), ifSource('dv360_data','dv360.date'), ifSource('stg_bing_ad_group_performance','CAST(microsoft.time_period AS DATE)'), ifSource('stg_linkedin_ads_combined','linkedin.startDate')])} as record_date,
        ${ifNull(['CAST(google_ads.ad_group_id AS STRING)', ifSource('stg_facebookdata','facebook.adset_id'), ifSource('dv360_data','CAST(dv360.line_item_id AS STRING)'), ifSource('stg_bing_ad_group_performance','microsoft.adgroup_id'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_group_id')])} as ad_group_id,
        ${ifNull(['google_ads.ad_group_name', ifSource('stg_facebookdata','facebook.adset_name'), ifSource('dv360_data','dv360.line_item'), ifSource('stg_bing_ad_group_performance','microsoft.adgroup_name'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_group_name')])} as ad_group_name,
        ${ifNull(['google_ads.impressions', ifSource('stg_facebookdata','facebook.impressions'), ifSource('dv360_data','dv360.impressions'), ifSource('stg_bing_ad_group_performance','microsoft.impressions'), ifSource('stg_linkedin_ads_combined','linkedin.impressions')])} as ads_impressions,
        ${ifNull(['google_ads.interactions', ifSource('stg_facebookdata','facebook.link_click'), ifSource('dv360_data','dv360.clicks'), ifSource('stg_bing_ad_group_performance','microsoft.clicks'), ifSource('stg_linkedin_ads_combined','linkedin.interactions')])} as ads_interactions,
        ${ifNull(['google_ads.conversions', ifSource('stg_facebookdata','facebook.lead'), ifSource('dv360_data','dv360.click_through_conversions'), ifSource('stg_bing_ad_group_performance','microsoft.all_conversions'), ifSource('stg_linkedin_ads_combined','linkedin.website_conversions')])} as ads_conversions,
        ${ifNull(['google_ads.Cost', ifSource('stg_facebookdata','facebook.spend'), ifSource('dv360_data','dv360.revenue_advertiser_currency'), ifSource('stg_bing_ad_group_performance','microsoft.spend'), ifSource('stg_linkedin_ads_combined','linkedin.cost')])} as ads_cost,
        ${ifNull(['google_ads.campaign_advertising_channel_type', ifSource('stg_bing_ad_group_performance','microsoft.campaign_type'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_cost_type')])} AS campaign_advertising_channel_type,
        ${ifNull(['google_ads.campaign_bidding_strategy_type', ifSource('dv360_data','line_item_type'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_optimization_target_type')])} AS bidding_strategy_type,
        ${ifNull(['google_ads.campaign_status', ifSource('stg_bing_ad_group_performance','microsoft.campaign_status'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_status')])} AS campaign_status,
        ${ifNull(['google_ads.ad_group_status', ifSource('dv360_data','dv360.insertion_order_status'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_group_status')])} AS ad_group_status,
        ${ifNull(['google_ads.ad_group_type', ifSource('stg_bing_ad_group_performance','microsoft.adgroup_type')])} AS ad_group_type,
        google_ads.conversions_value, 
        ${ifNull([ifSource('stg_facebookdata','facebook.objective'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_objective_type')])} AS objective
        ${ifSource("stg_facebookdata", "facebook.ad_id AS facebook_ad_id,")}
        ${ifSource("stg_facebookdata", "facebook.ad_name AS facebook_ad_name,")}
        ${ifSource("dv360_data", "dv360.creative AS dv360_creative,")}
        ${ifSource("dv360_data", "dv360.creative_size AS dv360_creative_size,")}
        ${ifSource("dv360_data", "dv360.creative_type AS dv360_creative_type,")}
        ${ifSource("dv360_data", "dv360.site AS dv360_site,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_plays AS dv360_rich_media_video_plays,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_pauses AS dv360_rich_media_video_pauses,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_skips AS dv360_rich_media_video_skips,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_mutes AS dv360_rich_media_video_mutes,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_first_quartile_completes AS dv360_rich_media_video_first_quartile_completes,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_midpoints AS dv360_rich_media_video_midpoints,")}
        ${ifSource("dv360_data", "dv360.rich_media_video_third_quartile_completes AS dv360_rich_media_video_third_quartile_completes,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_plays AS dv360_rich_media_audio_plays,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_pauses AS dv360_rich_media_audio_pauses,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_stops AS dv360_rich_media_audio_stops,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_mutes AS dv360_rich_media_audio_mutes,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_first_quartile_completes AS dv360_rich_media_audio_first_quartile_completes,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_midpoints AS dv360_rich_media_audio_midpoints,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_third_quartile_completes AS dv360_rich_media_audio_third_quartile_completes,")}
        ${ifSource("dv360_data", "dv360.rich_media_audio_completions AS dv360_rich_media_audio_completions,")}
        ${ifSource("stg_bing_ad_group_performance", "microsoft.quality_score,")}
        ${ifSource("stg_linkedin_ads_combined", "linkedin.comments,")}
        ${ifSource("stg_linkedin_ads_combined", "linkedin.shares,")}
        ${ifSource("stg_linkedin_ads_combined", "linkedin.reactions,")}
        ${ifSource("stg_linkedin_ads_combined", "linkedin.likes,")}
        ${ifSource("stg_linkedin_ads_combined", "linkedin.download_clicks,")}

    FROM 
        ${ref("df_staging_views", "stg_google_ads_adgroup_combined")} google_ads

    ${join("full outer join", "df_staging_views", "stg_facebookdata", "AS facebook ON 1=0")}
    ${join("full outer join", "df_rawdata_views", "dv360_data", "AS dv360 ON 1=0")}
    ${join("full outer join", "df_staging_views", "stg_bing_ad_group_performance", "AS microsoft ON 1=0")}
    ${join("full outer join", "df_staging_views", "stg_linkedin_ads_combined", "AS linkedin ON 1=0")}
)
    `
let refs = getRefs()
module.exports = {query, refs}