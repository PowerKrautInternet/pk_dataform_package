/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `


    SELECT
        'DV360' as bron,
        account,
        advertiser_id,
        campaign_id,
        MAX(advertiser) AS advertiser,
        MAX(advertiser_currency) AS advertiser_currency,
        insertion_order_id,
        MAX(insertion_order) AS insertion_order,
        MAX(insertion_order_status) AS insertion_order_status,
        line_item_id,
        MAX(line_item) AS line_item,
        MAX(line_item_type) AS line_item_type,
        MAX(line_item_status) AS line_item_status,
        MAX(line_item_lifetime_frequency) AS line_item_lifetime_frequency,
        creative_id,
        MAX(creative) AS creative,
        MAX(creative_size) AS creative_size,
        MAX(creative_type) AS creative_type,
        date,
        site_id,
        MAX(site) AS site,
        SUM(CAST(revenue_advertiser_currency AS FLOAT64)) AS revenue_advertiser_currency,
        SUM(CAST(media_cost_advertiser_currency AS FLOAT64)) AS media_cost_advertiser_currency,
        SUM(CAST(impressions AS INT64)) AS impressions,
        SUM(CAST(active_view_viewable_impressions AS INT64)) AS active_view_viewable_impressions,
        SUM(CAST(clicks AS INT64)) AS clicks,
        SUM(CAST(total_conversions AS INT64)) AS total_conversions,
        SUM(CAST(view_through_conversions AS FLOAT64)) AS view_through_conversions,
        SUM(CAST(click_through_conversions AS FLOAT64)) AS click_through_conversions,
        SUM(CAST(rich_media_video_plays AS FLOAT64)) AS rich_media_video_plays,
        SUM(CAST(rich_media_video_pauses AS FLOAT64)) AS rich_media_video_pauses,
        SUM(CAST(rich_media_video_skips AS FLOAT64)) AS rich_media_video_skips,
        SUM(CAST(rich_media_video_mutes AS FLOAT64)) AS rich_media_video_mutes,
        SUM(CAST(rich_media_video_first_quartile_completes AS FLOAT64)) AS rich_media_video_first_quartile_completes,
        SUM(CAST(rich_media_video_midpoints AS FLOAT64)) AS rich_media_video_midpoints,
        SUM(CAST(rich_media_video_third_quartile_completes AS FLOAT64)) AS rich_media_video_third_quartile_completes,
        SUM(CAST(rich_media_audio_plays AS FLOAT64)) AS rich_media_audio_plays,
        SUM(CAST(rich_media_audio_pauses AS FLOAT64)) AS rich_media_audio_pauses,
        SUM(CAST(rich_media_audio_stops AS FLOAT64)) AS rich_media_audio_stops,
        SUM(CAST(rich_media_audio_mutes AS FLOAT64)) AS rich_media_audio_mutes,
        SUM(CAST(rich_media_audio_first_quartile_completes AS FLOAT64)) AS rich_media_audio_first_quartile_completes,
        SUM(CAST(rich_media_audio_midpoints AS FLOAT64)) AS rich_media_audio_midpoints,
        SUM(CAST(rich_media_audio_third_quartile_completes AS FLOAT64)) AS rich_media_audio_third_quartile_completes,
        SUM(CAST(rich_media_audio_completions AS FLOAT64)) AS rich_media_audio_completions,

    FROM (SELECT DISTINCT * FROM ${ref("DV360")})

    GROUP BY
        account,
        advertiser_id,
        campaign_id,
        insertion_order_id,
        line_item_id,
        creative_id,
        date,
        site_id

`
let refs = pk.getRefs()
module.exports = {query, refs}
