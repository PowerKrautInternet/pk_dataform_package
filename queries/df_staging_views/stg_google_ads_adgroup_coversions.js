config {
    type: "view",
    schema: "df_staging_views"
}

SELECT 
    ad_group.customer_id,
    ad_group.campaign_id,
    MAX(ad_campaign.campaign_name) AS campaign_name,
    MAX(ad_campaign.campaign_advertising_channel_type) AS campaign_advertising_channel_type,
    MAX(ad_campaign.campaign_bidding_strategy_type) AS campaign_bidding_strategy_type,
    MAX(ad_campaign.campaign_start_date) AS campaign_start_date,
    MAX(ad_campaign.campaign_end_date) AS campaign_end_date,
    MAX(ad_campaign.campaign_status) AS campaign_status,
    ad_group.ad_group_id,
    MAX(ad_group.ad_group_name) AS ad_group_name,
    MAX(ad_group_status) AS ad_group_status,
    MAX(ad_group_type) AS ad_group_type,
    MAX(ad_group.campaign_bidding_strategy_type) AS ad_group_bidding_strategy_type,
    ad_group_conversions.segments_date AS segments_date,
    ad_group_conversions.segments_device,
    ad_group_conversions.segments_conversion_action_name,
    SUM(ad_group_conversions.metrics_conversions) AS conversions,
    SUM(ad_group_conversions.metrics_conversions_value) AS conversions_value

FROM ${ref('ads_AdGroup_7594935172')} ad_group 

LEFT JOIN ${ref('ads_AdGroupConversionStats_7594935172')} ad_group_conversions
ON
  ad_group.customer_id = ad_group_conversions.customer_id 
  AND ad_group.campaign_id = ad_group_conversions.campaign_id
  AND ad_group.ad_group_id = ad_group_conversions.ad_group_id

LEFT JOIN (SELECT * FROM ${ref('ads_Campaign_7594935172')} WHERE _DATA_DATE = _LATEST_DATE) ad_campaign 
ON 
    ad_group.customer_id = ad_campaign.customer_id 
    AND ad_group.campaign_id = ad_campaign.campaign_id

WHERE
ad_group._DATA_DATE = ad_group._LATEST_DATE
AND ad_group_conversions.segments_date IS NOT NULL

GROUP BY
    customer_id,
    campaign_id,
    ad_group_id,
    segments_date,
    segments_device,
    segments_conversion_action_name

