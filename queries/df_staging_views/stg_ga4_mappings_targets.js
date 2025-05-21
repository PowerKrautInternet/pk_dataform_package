/*config*/
const {ref, join, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
    

SELECT 
    ga4.* EXCEPT(merk_session, kanaal, sessie_conversie_bron),
    CASE 
        WHEN merk_session IN ('vw-bedrijfswagens', 'volkswagen-bedrijfswagens') THEN 'Volkswagen Bedrijfswagens'
        WHEN merk_session IN ('VW', 'Volkswagen') THEN 'Volkswagen'
        ELSE merk_session 
    END AS merk_session,
    IFNULL(sessie_conversie_bron, kanaal) as kanaal,
    ac.mapping_thema,
    ac.mapping_edm as ac_campaign,
    ac.mapping_flows AS ac_name,
    ac.workflow_edm AS ac_workflow_edm

FROM(
SELECT
    * EXCEPT(event_ga_session_id, conversie_mapping 
        ${ifSource("stg_pivot_targets", "target_soort_conversie,")} 
        ${ifSource('stg_pivot_targets','targets.kanaal,')}, 
        target_record_datum, kanaal, event_date),
    IF(event_name <> "" AND standaard_event = 0, 1, 0) AS conversion_event,
    IF(user_pseudo_id IS NULL AND CAST(event_ga_session_id AS STRING) IS NULL AND event_name <> "" AND standaard_event = 0, unique_event_id, NULL) as privacy_conversion_id, 
    ${ifNull([
        ifSource("ga_conversie_mapping", "conversie_mapping"),
        ifSource("gs_ga4_standaard_events", `IF(standaard_event = 0, event_name, NULL)`),
        ifSource("stg_pivot_targets", "target_soort_conversie")
    ])} as soort_conversie,
    ${ifNull([
        "kanaal",
        ifSource("stg_pivot_targets", "target_kanaal"),    
    ])} as kanaal,
    ${ifNull([
        "event_date",
        ifSource("stg_pivot_targets", "target_record_datum"),
    ])} as kanaal,
    CASE
        WHEN regexp_contains(session_source,'dv360') 
        OR regexp_contains(session_medium,'^(.*cpm.*)$') THEN 'DV360'
        WHEN regexp_contains(session_source,'facebook|Facebook|fb|instagram|ig|meta')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|.*paid.*)$') THEN 'Facebook'
        WHEN regexp_contains(session_source,'linkedin')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'LinkedIn'
        WHEN regexp_contains(session_source,'google|adwords')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Google Ads'
        WHEN regexp_contains(session_source,'bing')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Microsoft Ads'
        WHEN regexp_contains(session_source,'ActiveCampaign') THEN 'ActiveCampaign'
        ELSE NULL
    END AS sessie_conversie_bron

  FROM(
    SELECT 
      events_sessies.* EXCEPT(session_source, session_medium, session_campaign),
      IFNULL(CAST(event_ga_session_id AS STRING), privacy_session_id) as ga_session_id,
      IFNULL(session_source, first_user_source) as session_source,
      IFNULL(session_medium, first_user_medium) as session_medium,
      IFNULL(session_campaign, first_user_campaign_name) as session_campaign,
      ${ifSource('gs_ga4_standaard_events','standaard_event.event_name as event_name_standaard,')}
      ${ifSource('gs_ga4_standaard_events','IF(standaard_event.event_name <> "", 1, 0) AS standaard_event,')}
      ga_mapping.conversie_mapping,
      ga_mapping.telmethode as conversie_telmethode,
      ga_mapping.softhard as conversie_soft_hard, 
      ${ifSource('stg_pivot_targets','targets.conversie_mapping as target_soort_conversie')}
      ${ifSource('stg_pivot_targets','targets.kanaal as target_kanaal,')}
      ${ifSource('stg_pivot_targets', 'targets.record_datum as target_record_datum,')}
      ${ifSource('stg_pivot_targets', 'CAST(targets.day_target AS INT64) AS conversie_target,')}
    
    FROM ${ref("df_staging_tables", "stg_ga4_events_sessies")} events_sessies
    
    ${join("left join","gs_ga4_standaard_events", "AS standaard_event ON TRIM(events_sessies.event_name) = TRIM(standaard_event.event_name)")}
    ${join("left join","ga_conversie_mapping", "AS ga_mapping ON TRIM(events_sessies.event_name) = TRIM(ga_mapping.event_name)")}
    ${join("full Outer Join","df_staging_views", "stg_pivot_targets", "AS targets ON 1=0")}
  )
) ga4 
  
${join("LEFT JOIN", "gs_activecampaign_ga4_mapping", "AS ac ON ac.session_campaign = ga4.session_campaign")}
`

let refs = getRefs()
module.exports = {query, refs}