/*config*/
let pk = require("../../sources")
let sources = pk.getSources().map((s) => s.alias ?? s.name )
let ref = pk.ref
let query = `

WITH sequences AS(
  SELECT
    account,
    json_value(PAYLOAD, '$.pk_crm_id') AS pk_crm_id,
    json_value(PAYLOAD, '$.type') AS type,
    json_value(PAYLOAD, '$.account_id') AS account_id,
    json_value(PAYLOAD, '$.ad_id') AS ad_id,
    json_value(PAYLOAD, '$.adset_id') AS adset_id,
    json_value(PAYLOAD, '$.campaign_id') AS campaign_id,
    json_value(PAYLOAD, '$.date_start') AS date_start,
    json_value(PAYLOAD, '$.response.account_currency') AS account_currency,
    json_value(PAYLOAD, '$.response.account_name') AS account_name,
    json_value(PAYLOAD, '$.response.ad_click_actions') AS ad_click_actions,
    json_value(PAYLOAD, '$.response.ad_impression_actions') AS ad_impression_actions,
    json_value(PAYLOAD, '$.response.ad_name') AS ad_name,
    json_value(PAYLOAD, '$.response.adset_end') AS adset_end,
    json_value(PAYLOAD, '$.response.adset_name') AS adset_name,
    json_value(PAYLOAD, '$.response.adset_start') AS adset_start,
    json_value(PAYLOAD, '$.response.age_targeting') AS age_targeting,
    json_value(PAYLOAD, '$.response.attribution_setting') AS attribution_setting,
    json_value(PAYLOAD, '$.response.auction_bid') AS auction_bid,
    json_value(PAYLOAD, '$.response.auction_competitiveness') AS auction_competitiveness,
    json_value(PAYLOAD, '$.response.auction_max_competitor_bid') AS auction_max_competitor_bid,
    json_value(PAYLOAD, '$.response.buying_type') AS buying_type,
    json_value(PAYLOAD, '$.response.campaign_name') AS campaign_name,
    json_value(PAYLOAD, '$.response.canvas_avg_view_percent') AS canvas_avg_view_percent,
    json_value(PAYLOAD, '$.response.canvas_avg_view_time') AS canvas_avg_view_time,
    json_value(PAYLOAD, '$.response.catalog_segment_actions') AS catalog_segment_actions,
    json_value(PAYLOAD, '$.response.catalog_segment_value') AS catalog_segment_value,
    json_value(PAYLOAD, '$.response.catalog_segment_value_mobile_purchase_roas') AS catalog_segment_value_mobile_purchase_roas,
    json_value(PAYLOAD, '$.response.catalog_segment_value_omni_purchase_roas') AS catalog_segment_value_omni_purchase_roas,
    json_value(PAYLOAD, '$.response.catalog_segment_value_website_purchase_roas') AS catalog_segment_value_website_purchase_roas,
    json_value(PAYLOAD, '$.response.clicks') AS clicks,
    json_value(PAYLOAD, '$.response.conversion_rate_ranking') AS conversion_rate_ranking,
    json_value(PAYLOAD, '$.response.conversions') AS conversions,
    json_value(PAYLOAD, '$.response.converted_product_quantity') AS converted_product_quantity,
    json_value(PAYLOAD, '$.response.converted_product_value') AS converted_product_value,
    json_value(PAYLOAD, '$.response.cost_per_15_sec_video_view') AS cost_per_15_sec_video_view,
    json_value(PAYLOAD, '$.response.cost_per_2_sec_continuous_video_view') AS cost_per_2_sec_continuous_video_view,
    json_value(PAYLOAD, '$.response.cost_per_action_type') AS cost_per_action_type,
    json_value(PAYLOAD, '$.response.cost_per_ad_click') AS cost_per_ad_click,
    json_value(PAYLOAD, '$.response.cost_per_conversion') AS cost_per_conversion,
    json_value(PAYLOAD, '$.response.cost_per_dda_countby_convs') AS cost_per_dda_countby_convs,
    json_value(PAYLOAD, '$.response.cost_per_estimated_ad_recallers') AS cost_per_estimated_ad_recallers,
    json_value(PAYLOAD, '$.response.cost_per_inline_link_click') AS cost_per_inline_link_click,
    json_value(PAYLOAD, '$.response.cost_per_inline_post_engagement') AS cost_per_inline_post_engagement,
    json_value(PAYLOAD, '$.response.cost_per_one_thousand_ad_impression') AS cost_per_one_thousand_ad_impression,
    json_value(PAYLOAD, '$.response.cost_per_outbound_click') AS cost_per_outbound_click,
    json_value(PAYLOAD, '$.response.cost_per_thruplay') AS cost_per_thruplay,
    json_value(PAYLOAD, '$.response.cost_per_unique_action_type') AS cost_per_unique_action_type,
    json_value(PAYLOAD, '$.response.cost_per_unique_click') AS cost_per_unique_click,
    json_value(PAYLOAD, '$.response.cost_per_unique_conversion') AS cost_per_unique_conversion,
    json_value(PAYLOAD, '$.response.cost_per_unique_inline_link_click') AS cost_per_unique_inline_link_click,
    json_value(PAYLOAD, '$.response.cost_per_unique_outbound_click') AS cost_per_unique_outbound_click,
    json_value(PAYLOAD, '$.response.cpc') AS cpc,
    json_value(PAYLOAD, '$.response.cpm') AS cpm,
    json_value(PAYLOAD, '$.response.cpp') AS cpp,
    json_value(PAYLOAD, '$.response.created_time') AS created_time,
    json_value(PAYLOAD, '$.response.creative_media_type') AS creative_media_type,
    json_value(PAYLOAD, '$.response.ctr') AS ctr,
    json_value(PAYLOAD, '$.response.date_stop') AS date_stop,
    json_value(PAYLOAD, '$.response.dda_countby_convs') AS dda_countby_convs,
    json_value(PAYLOAD, '$.response.dda_results') AS dda_results,
    json_value(PAYLOAD, '$.response.engagement_rate_ranking') AS engagement_rate_ranking,
    json_value(PAYLOAD, '$.response.estimated_ad_recall_rate') AS estimated_ad_recall_rate,
    json_value(PAYLOAD, '$.response.estimated_ad_recall_rate_lower_bound') AS estimated_ad_recall_rate_lower_bound,
    json_value(PAYLOAD, '$.response.estimated_ad_recall_rate_upper_bound') AS estimated_ad_recall_rate_upper_bound,
    json_value(PAYLOAD, '$.response.estimated_ad_recallers') AS estimated_ad_recallers,
    json_value(PAYLOAD, '$.response.estimated_ad_recallers_lower_bound') AS estimated_ad_recallers_lower_bound,
    json_value(PAYLOAD, '$.response.estimated_ad_recallers_upper_bound') AS estimated_ad_recallers_upper_bound,
    json_value(PAYLOAD, '$.response.frequency') AS frequency,
    json_value(PAYLOAD, '$.response.full_view_impressions') AS full_view_impressions,
    json_value(PAYLOAD, '$.response.full_view_reach') AS full_view_reach,
    json_value(PAYLOAD, '$.response.gender_targeting') AS gender_targeting,
    json_value(PAYLOAD, '$.response.impressions') AS impressions,
    json_value(PAYLOAD, '$.response.inline_link_click_ctr') AS inline_link_click_ctr,
    json_value(PAYLOAD, '$.response.inline_link_clicks') AS inline_link_clicks,
    json_value(PAYLOAD, '$.response.inline_post_engagement') AS inline_post_engagement,
    json_value(PAYLOAD, '$.response.instagram_upcoming_event_reminders_set') AS instagram_upcoming_event_reminders_set,
    json_value(PAYLOAD, '$.response.instant_experience_clicks_to_open') AS instant_experience_clicks_to_open,
    json_value(PAYLOAD, '$.response.instant_experience_clicks_to_start') AS instant_experience_clicks_to_start,
    json_value(PAYLOAD, '$.response.instant_experience_outbound_clicks') AS instant_experience_outbound_clicks,
    json_value(PAYLOAD, '$.response.interactive_component_tap') AS interactive_component_tap,
    json_value(PAYLOAD, '$.response.labels') AS labels,
    json_value(PAYLOAD, '$.response.location') AS location,
    json_value(PAYLOAD, '$.response.marketing_messages_cost_per_delivered') AS marketing_messages_cost_per_delivered,
    json_value(PAYLOAD, '$.response.marketing_messages_cost_per_link_btn_click') AS marketing_messages_cost_per_link_btn_click,
    json_value(PAYLOAD, '$.response.marketing_messages_spend') AS marketing_messages_spend,
    json_value(PAYLOAD, '$.response.mobile_app_purchase_roas') AS mobile_app_purchase_roas,
    json_value(PAYLOAD, '$.response.objective') AS objective,
    json_value(PAYLOAD, '$.response.optimization_goal') AS optimization_goal,
    json_value(PAYLOAD, '$.response.outbound_clicks') AS outbound_clicks,
    json_value(PAYLOAD, '$.response.outbound_clicks_ctr') AS outbound_clicks_ctr,
    json_value(PAYLOAD, '$.response.place_page_name') AS place_page_name,
    json_value(PAYLOAD, '$.response.purchase_roas') AS purchase_roas,
    json_value(PAYLOAD, '$.response.qualifying_question_qualify_answer_rate') AS qualifying_question_qualify_answer_rate,
    json_value(PAYLOAD, '$.response.quality_ranking') AS quality_ranking,
    json_value(PAYLOAD, '$.response.reach') AS reach,
    json_value(PAYLOAD, '$.response.social_spend') AS social_spend,
    json_value(PAYLOAD, '$.response.spend') AS spend,
    json_value(PAYLOAD, '$.response.total_postbacks') AS total_postbacks,
    json_value(PAYLOAD, '$.response.total_postbacks_detailed') AS total_postbacks_detailed,
    json_value(PAYLOAD, '$.response.total_postbacks_detailed_v4') AS total_postbacks_detailed_v4,
    json_value(PAYLOAD, '$.response.unique_actions') AS unique_actions,
    json_value(PAYLOAD, '$.response.unique_clicks') AS unique_clicks,
    json_value(PAYLOAD, '$.response.unique_conversions') AS unique_conversions,
    json_value(PAYLOAD, '$.response.unique_ctr') AS unique_ctr,
    json_value(PAYLOAD, '$.response.unique_inline_link_click_ctr') AS unique_inline_link_click_ctr,
    json_value(PAYLOAD, '$.response.unique_inline_link_clicks') AS unique_inline_link_clicks,
    json_value(PAYLOAD, '$.response.unique_link_clicks_ctr') AS unique_link_clicks_ctr,
    json_value(PAYLOAD, '$.response.unique_outbound_clicks') AS unique_outbound_clicks,
    json_value(PAYLOAD, '$.response.unique_outbound_clicks_ctr') AS unique_outbound_clicks_ctr,
    json_value(PAYLOAD, '$.response.unique_video_continuous_2_sec_watched_actions') AS unique_video_continuous_2_sec_watched_actions,
    json_value(PAYLOAD, '$.response.unique_video_view_15_sec') AS unique_video_view_15_sec,
    json_value(PAYLOAD, '$.response.updated_time') AS updated_time,
    json_value(PAYLOAD, '$.response.video_15_sec_watched_actions') AS video_15_sec_watched_actions,
    json_value(PAYLOAD, '$.response.video_30_sec_watched_actions') AS video_30_sec_watched_actions,
    json_value(PAYLOAD, '$.response.video_avg_time_watched_actions') AS video_avg_time_watched_actions,
    json_value(PAYLOAD, '$.response.video_continuous_2_sec_watched_actions') AS video_continuous_2_sec_watched_actions,
    json_value(PAYLOAD, '$.response.video_p100_watched_actions') AS video_p100_watched_actions,
    json_value(PAYLOAD, '$.response.video_p25_watched_actions') AS video_p25_watched_actions,
    json_value(PAYLOAD, '$.response.video_p50_watched_actions') AS video_p50_watched_actions,
    json_value(PAYLOAD, '$.response.video_p75_watched_actions') AS video_p75_watched_actions,
    json_value(PAYLOAD, '$.response.video_p95_watched_actions') AS video_p95_watched_actions,
    json_value(PAYLOAD, '$.response.video_play_actions') AS video_play_actions,
    json_value(PAYLOAD, '$.response.video_play_curve_actions') AS video_play_curve_actions,
    json_value(PAYLOAD, '$.response.video_play_retention_0_to_15s_actions') AS video_play_retention_0_to_15s_actions,
    json_value(PAYLOAD, '$.response.video_play_retention_20_to_60s_actions') AS video_play_retention_20_to_60s_actions,
    json_value(PAYLOAD, '$.response.video_play_retention_graph_actions') AS video_play_retention_graph_actions,
    json_value(PAYLOAD, '$.response.video_thruplay_watched_actions') AS video_thruplay_watched_actions,
    json_value(PAYLOAD, '$.response.video_time_watched_actions') AS video_time_watched_actions,
    json_value(PAYLOAD, '$.response.website_purchase_roas') AS website_purchase_roas,
    json_value(PAYLOAD, '$.response.wish_bid') AS wish_bid,
    json_query_array(PAYLOAD, '$.response.actions') AS actions
  FROM
    ${ref("df_rawdata_views", "facebookDataProducer_lasttransaction")})

  SELECT 
    sequences.*,
    actions_nieuw
  FROM
    sequences
  CROSS JOIN
    UNNEST(actions) AS actions_nieuw

`

if(!sources.includes("facebookDataProducer_lasttransaction")){query = `ERROR: facebook is geen bron!`}

let refs = pk.getRefs()
module.exports = {query, refs}
