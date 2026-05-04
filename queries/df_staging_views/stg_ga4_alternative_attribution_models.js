/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
WITH
  -- 1. Basisgegevens en attributen van sessies en conversies combineren
  BaseData AS (
  SELECT
    conversies.unique_event_id AS conversie_event_id,
    conversies.account AS account,
    conversies.event_name AS conversie_event_name,
    PARSE_DATE('%Y%m%d', conversies.event_date) AS conversie_event_date,
    sessies.user_pseudo_id,
    sessies.event_ga_session_id,
    sessies.event_timestamp,
    -- Bepalen van de session_source en session_medium
    COALESCE(
      IF(
        sessies.event_gclid <> "",
        sessies.cross_channel_campaign_last_click_source,
        sessies.collected_traffic_source_manual_source
      ),
      IF(
        sessies.event_gclid <> "",
        sessies.collected_traffic_source_manual_source,
        sessies.cross_channel_campaign_last_click_source
      ),
      '(direct)' -- Gebruik (direct) als fallback
    ) AS session_source,
    COALESCE(
      IF(
        sessies.event_gclid <> "",
        sessies.cross_channel_campaign_last_click_medium,
        sessies.collected_traffic_source_manual_medium
      ),
      IF(
        sessies.event_gclid <> "",
        sessies.collected_traffic_source_manual_medium,
        sessies.cross_channel_campaign_last_click_medium
      ),
      '(none)' -- Gebruik (none) als fallback
    ) AS session_medium,
    -- Window Functions voor attributie:
    COUNT(*) OVER (PARTITION BY sessies.user_pseudo_id, conversies.unique_event_id) AS total_interactions_per_conversion,
    ROW_NUMBER() OVER (PARTITION BY sessies.user_pseudo_id, conversies.unique_event_id ORDER BY sessies.event_timestamp) AS interaction_number,
    ROW_NUMBER() OVER (PARTITION BY sessies.user_pseudo_id, conversies.unique_event_id ORDER BY sessies.event_timestamp DESC) AS interaction_number_desc
  FROM
    ${ref("df_staging_views", "stg_ga4_attribution_model_conversies")} AS conversies
  LEFT JOIN
    ${ref("df_staging_views", "stg_ga4_attribution_model_sessies")} AS sessies
    ON conversies.user_pseudo_id = sessies.user_pseudo_id
  WHERE
    PARSE_DATE('%Y%m%d', sessies.event_date) > DATE_SUB(PARSE_DATE('%Y%m%d', conversies.event_date), INTERVAL 30 DAY)
    AND PARSE_DATE('%Y%m%d', sessies.event_date) <= PARSE_DATE('%Y%m%d', conversies.event_date)
  ),

  -- 2. Aggregeren per sessie/conversie, attributiepunten toekennen
  AggregatedData AS (
  SELECT
    user_pseudo_id, 
    account,
    conversie_event_id,
    conversie_event_name,
    conversie_event_date,
    session_source,
    session_medium,
    MAX(
      IF(interaction_number = 1, 1, NULL)
    ) AS is_first_click,
    MAX(
      IF(interaction_number_desc = 1, 1, NULL)
    ) AS is_last_click,
    -- Berekeningen op geaggregeerd niveau:
    COUNT(DISTINCT event_ga_session_id) AS count_sessions,
    SUM(total_interactions_per_conversion) AS total_interactions_for_source_medium,
    -- Bereken de complexere 'position_conversies' logica
    CASE
      WHEN COUNT(DISTINCT CONCAT(session_source, session_medium)) OVER (PARTITION BY user_pseudo_id, conversie_event_id) = 1 THEN 1.0
      WHEN COUNT(DISTINCT CONCAT(session_source, session_medium)) OVER (PARTITION BY user_pseudo_id, conversie_event_id) = 2 THEN 0.5
      WHEN MIN(interaction_number) = 1 THEN 0.4
      WHEN MIN(interaction_number_desc) = 1 THEN 0.4
      ELSE 0.2
    END AS position_conversies
  FROM
    BaseData
  GROUP BY
    user_pseudo_id,
    conversie_event_id,
    conversie_event_name,
    conversie_event_date,
    session_source,
    session_medium,
    account
  ),

  -- 3. Attributieberekening voor het middendeel
  AttributionCalculation AS (
  SELECT
    *,
    -- Totale interacties van 'midden'-sessies (niet eerste/laatste)
    SUM(
      IF(
        is_first_click IS NULL AND is_last_click IS NULL,
        total_interactions_for_source_medium,
        NULL
      )
    ) OVER (PARTITION BY user_pseudo_id, conversie_event_id) AS total_middle_interactions_overall
  FROM
    AggregatedData
  )

-- 4. Finale SELECT met de gevraagde output
SELECT
  user_pseudo_id,
  conversie_event_id,
  conversie_event_name,
  conversie_event_date,
  session_source,
  account,
  session_medium,
  NULLIF(CONCAT(session_source,' / ', session_medium), '(direct) / (none)') AS session_source_medium,
  count_sessions,
  -- Bereken distribution_middle voor niet-eerste/laatste
  IF(
    is_first_click IS NULL AND is_last_click IS NULL,
    total_interactions_for_source_medium / total_middle_interactions_overall,
    NULL
  ) AS distribution_middle,
  -- Eindberekening van de position_based_attribution
  IF(
    is_first_click = 1 OR is_last_click = 1,
    position_conversies,
    (total_interactions_for_source_medium / total_middle_interactions_overall) * position_conversies
  ) AS position_based_attribution,
  is_first_click AS first_click_attribution,
  is_last_click AS last_click_attribution
FROM
  AttributionCalculation
`
let refs = getRefs()
module.exports = {query, refs}
