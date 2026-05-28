/*config*/
const {join, ref, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
WITH
  -- 1. Joint conversies aan voorafgaande sessies van dezelfde user binnen 30 dagen.
  --    JOIN op (user_pseudo_id, account) en filter op timestamp (niet date) zodat sessies
  --    NA de conversie op dezelfde dag niet meer worden meegeteld.
  Joined AS (
  SELECT
    conversies.unique_event_id AS conversie_event_id,
    conversies.account AS account,
    conversies.event_name AS conversie_event_name,
    SAFE.PARSE_DATE('%Y%m%d', conversies.event_date) AS conversie_event_date,
    conversies.event_timestamp AS conversie_event_timestamp,
    sessies.user_pseudo_id,
    sessies.event_ga_session_id,
    sessies.event_timestamp,
    sessies.event_gclid,
    sessies.cross_channel_campaign_last_click_source,
    sessies.cross_channel_campaign_last_click_medium,
    sessies.collected_traffic_source_manual_source,
    sessies.collected_traffic_source_manual_medium,
    -- Atomic source/medium-keuze: bron/medium komen uit dezelfde set, nooit gemengd.
    -- gclid bepaalt prioriteit (cross_channel > manual), zonder gclid omgekeerd.
    -- COALESCE(.., '') <> '' behandelt zowel NULL als lege string als "geen waarde".
    CASE
      WHEN sessies.event_gclid <> "" AND COALESCE(sessies.cross_channel_campaign_last_click_source, '') <> '' THEN 'cross_channel'
      WHEN sessies.event_gclid <> "" AND COALESCE(sessies.collected_traffic_source_manual_source,     '') <> '' THEN 'manual'
      WHEN COALESCE(sessies.collected_traffic_source_manual_source,     '') <> '' THEN 'manual'
      WHEN COALESCE(sessies.cross_channel_campaign_last_click_source,   '') <> '' THEN 'cross_channel'
      ELSE 'direct'
    END AS source_pick
  FROM
    ${ref("df_staging_views", "stg_ga4_attribution_model_conversies")} AS conversies
  LEFT JOIN
    ${ref("df_staging_views", "stg_ga4_attribution_model_sessies")} AS sessies
    ON conversies.user_pseudo_id = sessies.user_pseudo_id
   AND conversies.account = sessies.account
  WHERE
    sessies.event_timestamp <= conversies.event_timestamp
    AND SAFE.PARSE_DATE('%Y%m%d', sessies.event_date) > DATE_SUB(SAFE.PARSE_DATE('%Y%m%d', conversies.event_date), INTERVAL 30 DAY)
  ),

  -- 2. Resolveer atomic source/medium en bereken interaction-windows (regulier én non-direct).
  --    Voor non-direct windows worden directs achteraan gesorteerd: rang 1 onder is_direct=FALSE
  --    is dus de eerste/laatste non-direct sessie.
  BaseData AS (
  SELECT
    conversie_event_id,
    account,
    conversie_event_name,
    conversie_event_date,
    user_pseudo_id,
    event_ga_session_id,
    event_timestamp,
    CASE source_pick
      WHEN 'cross_channel' THEN cross_channel_campaign_last_click_source
      WHEN 'manual'        THEN collected_traffic_source_manual_source
      ELSE '(direct)'
    END AS session_source,
    CASE source_pick
      WHEN 'cross_channel' THEN cross_channel_campaign_last_click_medium
      WHEN 'manual'        THEN collected_traffic_source_manual_medium
      ELSE '(none)'
    END AS session_medium,
    (source_pick = 'direct') AS is_direct,
    -- Reguliere interaction windows (alle sessies tellen mee).
    COUNT(*) OVER (PARTITION BY user_pseudo_id, conversie_event_id) AS total_interactions_per_conversion,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, conversie_event_id ORDER BY event_timestamp) AS interaction_number,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, conversie_event_id ORDER BY event_timestamp DESC) AS interaction_number_desc,
    -- Non-direct windows: directs sorteren naar achteren via (source_pick = 'direct') als primaire ORDER BY-key.
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, conversie_event_id
      ORDER BY (source_pick = 'direct'), event_timestamp
    ) AS interaction_number_nd_asc,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, conversie_event_id
      ORDER BY (source_pick = 'direct'), event_timestamp DESC
    ) AS interaction_number_nd_desc,
    COUNTIF(source_pick <> 'direct') OVER (PARTITION BY user_pseudo_id, conversie_event_id) AS non_direct_session_count
  FROM Joined
  ),

  -- 3. Aggregeer per (user, conversie, source/medium) en bereken position_conversies-weights.
  --    Reguliere weights: 1.0 / 0.5 / 0.4 / 0.4 / 0.2 — met fix voor first==last (zelfde source/medium):
  --    die groep krijgt 0.8 zodat de som per conversie 1.0 blijft.
  AggregatedData AS (
  SELECT
    user_pseudo_id,
    account,
    conversie_event_id,
    conversie_event_name,
    conversie_event_date,
    session_source,
    session_medium,
    is_direct,
    MAX(IF(interaction_number = 1, 1, NULL)) AS is_first_click,
    MAX(IF(interaction_number_desc = 1, 1, NULL)) AS is_last_click,
    -- Non-direct first/last: alleen 1 wanneer de rij niet-direct is én het eerste/laatste
    -- non-direct rangnummer (rang 1) bevat. Door de IF NOT is_direct levert dit NULL voor directs.
    MAX(IF(interaction_number_nd_asc = 1 AND NOT is_direct, 1, NULL)) AS is_first_click_nd,
    MAX(IF(interaction_number_nd_desc = 1 AND NOT is_direct, 1, NULL)) AS is_last_click_nd,
    COUNT(DISTINCT event_ga_session_id) AS count_sessions,
    MAX(non_direct_session_count) AS non_direct_session_count,
    -- LET OP: misleidende naam, behoudt voor backwards-compat met downstream.
    -- Werkelijke waarde = aantal_sessies_in_sm × totaal_sessies_in_conversie.
    -- In de middle-ratio (teller/noemer) valt de "× totaal_sessies" weg, dus functioneel correct.
    SUM(total_interactions_per_conversion) AS total_interactions_for_source_medium,
    -- Helper voor non-direct middle-verdeling: 0 als deze rij direct is.
    SUM(IF(NOT is_direct, total_interactions_per_conversion, 0)) AS sm_interactions_nd_helper,

    -- Reguliere position_conversies, met first==last fix (0.8 wanneer dezelfde source/medium
    -- zowel eerst als laatst is — voorheen 0.4, waardoor som ≠ 1.0).
    CASE
      WHEN COUNT(DISTINCT CONCAT(session_source, session_medium)) OVER (PARTITION BY user_pseudo_id, conversie_event_id) = 1 THEN 1.0
      WHEN COUNT(DISTINCT CONCAT(session_source, session_medium)) OVER (PARTITION BY user_pseudo_id, conversie_event_id) = 2 THEN 0.5
      WHEN MIN(interaction_number) = 1 AND MIN(interaction_number_desc) = 1 THEN 0.8
      WHEN MIN(interaction_number) = 1 THEN 0.4
      WHEN MIN(interaction_number_desc) = 1 THEN 0.4
      ELSE 0.2
    END AS position_conversies,

    -- Non-direct position_conversies: identiek aan regulier, maar telt alleen niet-direct
    -- source/mediums. Direct-rijen krijgen 0.0 zolang er minstens één non-direct sessie is
    -- (fallback A regelt het 100%-direct geval in de Final SELECT).
    CASE
      WHEN is_direct AND MAX(non_direct_session_count) > 0 THEN 0.0
      WHEN COUNT(DISTINCT IF(is_direct, NULL, CONCAT(session_source, session_medium))) OVER (PARTITION BY user_pseudo_id, conversie_event_id) = 1 THEN 1.0
      WHEN COUNT(DISTINCT IF(is_direct, NULL, CONCAT(session_source, session_medium))) OVER (PARTITION BY user_pseudo_id, conversie_event_id) = 2 THEN 0.5
      WHEN MIN(IF(NOT is_direct, interaction_number_nd_asc, NULL)) = 1
       AND MIN(IF(NOT is_direct, interaction_number_nd_desc, NULL)) = 1 THEN 0.8
      WHEN MIN(IF(NOT is_direct, interaction_number_nd_asc,  NULL)) = 1 THEN 0.4
      WHEN MIN(IF(NOT is_direct, interaction_number_nd_desc, NULL)) = 1 THEN 0.4
      ELSE 0.2
    END AS position_conversies_nd
  FROM
    BaseData
  GROUP BY
    user_pseudo_id,
    account,
    conversie_event_id,
    conversie_event_name,
    conversie_event_date,
    session_source,
    session_medium,
    is_direct
  ),

  -- 4. Bereken middle-totalen op (user, conversie)-niveau voor zowel regulier als non-direct.
  AttributionCalculation AS (
  SELECT
    *,
    SUM(
      IF(
        is_first_click IS NULL AND is_last_click IS NULL,
        total_interactions_for_source_medium,
        NULL
      )
    ) OVER (PARTITION BY user_pseudo_id, conversie_event_id) AS total_middle_interactions_overall,
    SUM(
      IF(
        NOT is_direct AND is_first_click_nd IS NULL AND is_last_click_nd IS NULL,
        sm_interactions_nd_helper,
        NULL
      )
    ) OVER (PARTITION BY user_pseudo_id, conversie_event_id) AS total_middle_nd_interactions_overall
  FROM
    AggregatedData
  )

-- 5. Finale SELECT met de output-kolommen.
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
  IF(
    is_first_click IS NULL AND is_last_click IS NULL,
    total_interactions_for_source_medium / total_middle_interactions_overall,
    NULL
  ) AS distribution_middle,
  IF(
    is_first_click = 1 OR is_last_click = 1,
    position_conversies,
    (total_interactions_for_source_medium / total_middle_interactions_overall) * position_conversies
  ) AS position_based_attribution,
  is_first_click AS first_click_attribution,
  is_last_click AS last_click_attribution,

  -- Non-direct varianten met fallback A: bij 100%-direct conversie val terug op de reguliere
  -- waarde, zodat SUM(non_direct) per conversie ook altijd ~1.0 is.
  IF(non_direct_session_count = 0, is_first_click, is_first_click_nd) AS first_click_non_direct,
  IF(non_direct_session_count = 0, is_last_click,  is_last_click_nd)  AS last_click_non_direct,
  IF(
    non_direct_session_count = 0,
    IF(
      is_first_click = 1 OR is_last_click = 1,
      position_conversies,
      (total_interactions_for_source_medium / total_middle_interactions_overall) * position_conversies
    ),
    IF(
      is_direct,
      0.0,
      IF(
        is_first_click_nd = 1 OR is_last_click_nd = 1,
        position_conversies_nd,
        (sm_interactions_nd_helper / total_middle_nd_interactions_overall) * position_conversies_nd
      )
    )
  ) AS position_based_non_direct
FROM
  AttributionCalculation
`
let refs = getRefs()
module.exports = {query, refs}
