/*config*/
let pk = require("../../functions")
let ref = pk.ref
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
    * EXCEPT(event_ga_session_id, conversie_mapping, target_soort_conversie, target_kanaal, target_record_datum, kanaal, event_date),
    IF(event_name <> "" AND standaard_event = 0, 1, 0) AS conversion_event,
    IF(user_pseudo_id IS NULL AND CAST(event_ga_session_id AS STRING) IS NULL AND event_name <> "" AND standaard_event = 0, unique_event_id, NULL) as privacy_conversion_id, 
    IFNULL(
        IFNULL(conversie_mapping, IF(standaard_event = 0, event_name, NULL)), 
        target_soort_conversie) as soort_conversie,   
    IFNULL(kanaal, target_kanaal) AS kanaal,
    IFNULL(event_date, target_record_datum) AS event_date,
    CASE
        WHEN regexp_contains(session_source,'dv360') 
        OR regexp_contains(session_medium,'^(.*cpm.*)$') THEN 'DV360'
        WHEN regexp_contains(session_source,'facebook|Facebook|fb|instagram|ig|meta')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|.*paid.*)$') THEN 'Facebook'
        WHEN regexp_contains(session_source,'linkedin')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'LinkedIn'
        WHEN regexp_contains(session_source,'google|adwords')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Google Ads'
        --OR regexp_contains(session_campaign,'^(.*organic.*)$')) 
        WHEN regexp_contains(session_source,'bing')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Microsoft Ads'
        --OR regexp_contains(session_campaign,'^(.*organic.*)$')) THEN 'Microsoft Advertising'
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
standaard_event.event_name as event_name_standaard,
IF(standaard_event.event_name <> "", 1, 0) AS standaard_event,
ga_mapping.conversie_mapping,
ga_mapping.telmethode as conversie_telmethode,
ga_mapping.softhard as conversie_soft_hard, 
targets.conversie_mapping as target_soort_conversie,
targets.kanaal as target_kanaal,
targets.record_datum as target_record_datum,
CAST(targets.day_target AS INT64) AS conversie_target

FROM ${ref("df_staging_tables", "stg_ga4_events_sessies")} events_sessies

LEFT JOIN ${ref("gs_ga4_standaard_events")} standaard_event
ON TRIM(events_sessies.event_name) = TRIM(standaard_event.event_name)

LEFT JOIN ${ref("ga_conversie_mapping")} AS ga_mapping
ON TRIM(events_sessies.event_name) = TRIM(ga_mapping.event_name)

FULL OUTER JOIN (SELECT * FROM ${ref("df_staging_views", "stg_pivot_targets")}) targets
ON 1=0)) ga4

LEFT JOIN
  ${ref("gs_activecampaign_ga4_mapping")} ac
ON
  ac.session_campaign = ga4.session_campaign
`
let refs = pk.getRefs()
module.exports = {query, refs}