/*config*/
let {ref, getRefs, join, ifNull, ifSource, orSource} = require("../../sources")
let query = `

SELECT 
    * ${orSource(['googleads_campaignlabel', 'stg_bing_ad_group_performance'], 'EXCEPT (merk, model)')},
   ${ifNull([
        orSource(['googleads_campaignlabel', 'stg_bing_ad_group_performance'], 'merk'),
        ifSource("stg_handmatige_uitgaves_pivot", "uitgave_merk"),
        ifSource("gs_merken", `${ref("lookupTable")}(CONCAT(IFNULL(campaign_name, ''), ' ', IFNULL(ad_group_name, '')), TO_JSON_STRING(ARRAY(SELECT merk FROM ${ref("df_googlesheets_tables","gs_merken", true)})))`)
    ], "as merk,")}
    ${ifNull([
        orSource(['googleads_campaignlabel', 'stg_bing_ad_group_performance'], 'model'),
        ifSource('gs_modellen', `${ref("lookupTable")}(CONCAT(IFNULL(campaign_name, ''), ' ', IFNULL(ad_group_name, '')), INITCAP(TO_JSON_STRING(ARRAY(SELECT model FROM ${ref("df_googlesheets_tables", "gs_modellen", true)}))))`)
    ], "as model,")}
FROM (
    SELECT
        ${ifNull(['google_ads.bron', ifSource('stg_facebookdata','facebook.bron'), ifSource('dv360_data','dv360.bron'), ifSource('stg_bing_ad_group_performance','microsoft.bron'), ifSource('stg_linkedin_ads_combined','linkedin.bron'), ifSource('stg_vistar_media_ads','vistar_media.bron'), ifSource('stg_handmatige_uitgaves_pivot', 'handmatig.uitgave_bron')])} as bron,
        ${ifNull(['google_ads.account', ifSource('stg_facebookdata','facebook.account'), ifSource('dv360_data','dv360.account'), ifSource('stg_bing_ad_group_performance','microsoft.account'), ifSource('stg_linkedin_ads_combined','linkedin.account'), ifSource('stg_vistar_media_ads','vistar_media.account')])} as account,
        ${ifNull(['CAST(google_ads.account_id AS STRING)', ifSource('stg_facebookdata','facebook.account_id'), ifSource('dv360_data','CAST(dv360.advertiser_id AS STRING)'), ifSource('stg_bing_ad_group_performance','microsoft.account_id'), ifSource('stg_linkedin_ads_combined','linkedin.accountId'), ifSource('stg_vistar_media_ads','vistar_media.advertiser_name')])} as account_id,
        ${ifNull(['CAST(google_ads.account_id AS STRING)', ifSource('stg_facebookdata','facebook.account_name'), ifSource('dv360_data','dv360.advertiser'), ifSource('stg_bing_ad_group_performance','microsoft.account_name'), ifSource('stg_linkedin_ads_combined','linkedin.account_name'), ifSource('stg_vistar_media_ads','vistar_media.advertiser_name')])} as account_name,
        ${ifNull(['CAST(google_ads.campaign_id AS STRING)', ifSource('stg_facebookdata','facebook.campaign_id'), ifSource('dv360_data','CAST(dv360.insertion_order_id AS STRING)'), ifSource('stg_bing_ad_group_performance','microsoft.campaign_id'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_id'), ifSource('stg_vistar_media_ads','vistar_media.insertion_order')])} as campaign_id,
        ${ifNull(['google_ads.campaign_name', ifSource('stg_facebookdata','facebook.campaign_name'), ifSource('dv360_data','dv360.insertion_order'), ifSource('stg_bing_ad_group_performance','microsoft.campaign_name'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_name'), ifSource('stg_vistar_media_ads','vistar_media.insertion_order_name')])} as campaign_name,
        ${ifNull(['google_ads.segments_date', ifSource('stg_facebookdata','facebook.date_start'), ifSource('dv360_data','dv360.date'), ifSource('stg_bing_ad_group_performance','CAST(microsoft.time_period AS DATE)'), ifSource('stg_linkedin_ads_combined','linkedin.startDate'), ifSource('stg_handmatige_uitgaves_pivot', 'handmatig.record_datum'), ifSource('stg_vistar_media_ads','vistar_media.date')])} as record_date,
        ${ifNull(['CAST(google_ads.ad_group_id AS STRING)', ifSource('stg_facebookdata','facebook.adset_id'), ifSource('dv360_data','CAST(dv360.line_item_id AS STRING)'), ifSource('stg_bing_ad_group_performance','microsoft.adgroup_id'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_group_id'), ifSource('stg_vistar_media_ads','vistar_media.campaign_id')])} as ad_group_id,
        ${ifNull(['google_ads.ad_group_name', ifSource('stg_facebookdata','facebook.adset_name'), ifSource('dv360_data','dv360.line_item'), ifSource('stg_bing_ad_group_performance','microsoft.adgroup_name'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_group_name'), ifSource('stg_vistar_media_ads','vistar_media.campaign_name')])} as ad_group_name,
        ${ifNull(['google_ads.impressions', ifSource('stg_facebookdata','facebook.impressions'), ifSource('dv360_data','dv360.impressions'), ifSource('stg_bing_ad_group_performance','microsoft.impressions'), ifSource('stg_linkedin_ads_combined','linkedin.impressions'), ifSource('stg_vistar_media_ads','vistar_media.impressions')])} as ads_impressions,
        ${ifNull(['google_ads.clicks', ifSource('stg_facebookdata','facebook.link_click'), ifSource('dv360_data','dv360.clicks'), ifSource('stg_bing_ad_group_performance','microsoft.clicks'), ifSource('stg_linkedin_ads_combined','linkedin.clicks')])} as ads_interactions,  
        ${ifNull(['google_ads.conversions', ifSource('stg_facebookdata','facebook.lead'), ifSource('dv360_data','dv360.click_through_conversions'), ifSource('stg_bing_ad_group_performance','microsoft.all_conversions'), ifSource('stg_linkedin_ads_combined','linkedin.website_conversions')])} as ads_conversions,
        ${ifNull(['google_ads.Cost', ifSource('stg_facebookdata','facebook.spend'), ifSource('dv360_data','dv360.revenue_advertiser_currency'), ifSource('stg_bing_ad_group_performance','microsoft.spend'), ifSource('stg_linkedin_ads_combined','linkedin.cost'), ifSource('stg_handmatige_uitgaves_pivot', 'handmatig.uitgaven'), ifSource('stg_vistar_media_ads','vistar_media.client_cost')])} as ads_cost,
        ${ifNull(['google_ads.campaign_advertising_channel_type', ifSource('stg_bing_ad_group_performance','microsoft.campaign_type'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_cost_type')])} AS campaign_advertising_channel_type,
        ${ifNull(['google_ads.campaign_bidding_strategy_type', ifSource('dv360_data','line_item_type'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_optimization_target_type')])} AS bidding_strategy_type,
        ${ifNull(['google_ads.campaign_status', ifSource('stg_bing_ad_group_performance','microsoft.campaign_status'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_status')])} AS campaign_status,
        ${ifNull(['google_ads.ad_group_status', ifSource('dv360_data','dv360.insertion_order_status'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_group_status')])} AS ad_group_status,
        ${ifNull(['google_ads.ad_group_type', ifSource('stg_bing_ad_group_performance','microsoft.adgroup_type')], "AS ad_group_type,")}
        google_ads.conversions_value,
        google_ads.conversion_action_name,
        google_ads.interactions as google_ads_interactions,
        ${ifNull([ifSource('stg_facebookdata','facebook.objective'), ifSource('stg_linkedin_ads_combined','linkedin.campaign_objective_type')], "AS objective,")}
        ${ifSource("stg_facebookdata", "facebook.ad_id AS facebook_ad_id,")}
        ${ifSource("stg_facebookdata", "facebook.ad_name AS facebook_ad_name,")}
        ${ifSource("dv360_data", "dv360.view_through_conversions AS view_through_conversions,")}
        ${ifSource("dv360_data", "dv360.click_through_conversions AS click_through_conversions,")}
        ${ifSource("dv360_data", "dv360.active_view_viewable_impressions AS dv360_active_view_viewable_impressions,")}
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
        ${ifSource("stg_linkedin_ads_combined", "linkedin.interactions AS linkedin_interactions,")}
        ${ifSource('stg_handmatige_uitgaves_pivot', 'handmatig.uitgave_categorie,')} 
        ${ifSource('stg_handmatige_uitgaves_pivot', 'handmatig.uitgave_bron AS uitgave_bron_org,')} 
        ${ifSource('stg_handmatige_uitgaves_pivot', 'handmatig.uitgave_merk,')} 
        ${ifNull([ifSource('stg_bing_ad_group_performance','microsoft.campagnegroep'), ifSource('googleads_campaignlabel','google_ads.campagnegroep')], 'AS campagnegroep,')}
        ${ifNull([ifSource('stg_bing_ad_group_performance','microsoft.merk'), ifSource('googleads_campaignlabel','google_ads.merk')], 'AS merk,')}
        ${ifNull([ifSource('stg_bing_ad_group_performance','microsoft.model'), ifSource('googleads_campaignlabel','google_ads.model')], 'AS model,')}
        ${ifSource("stg_vistar_media_ads", "vistar_media.media_owner_display_name AS vistar_media_owner_display_name,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.venue_type AS vistar_media_venue_type,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.buy_type AS vistar_media_buy_type,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.country AS vistar_media_country,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.global_city AS vistar_media_global_city,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.deal AS vistar_media_deal,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.creative_name AS vistar_media_creative_name,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.spots AS vistar_media_spots,")}
        ${ifSource("stg_vistar_media_ads", "vistar_media.media_cost AS vistar_media_media_cost,")}
    FROM 
        ${ref("df_staging_views", "stg_googleads_combined")} google_ads

    ${join("full outer join", "df_staging_views", "stg_facebookdata", "AS facebook ON 1=0")}
    ${join("full outer join", "df_rawdata_views", "dv360_data", "AS dv360 ON 1=0")}
    ${join("full outer join", "df_staging_views", "stg_bing_ad_group_performance", "AS microsoft ON 1=0")}
    ${join("full outer join", "df_staging_views", "stg_linkedin_ads_combined", "AS linkedin ON 1=0")}
    ${join("full outer join", "df_staging_views", "stg_vistar_media_ads", "AS vistar_media ON 1=0")}
    ${join("full outer join", "df_staging_views", "stg_handmatige_uitgaves_pivot", "AS handmatig ON 1=0")}

)
    `
let refs = getRefs()
module.exports = {query, refs}
