/*config*/
const {ref, join, getRefs, ifSource, ifNull} = require("../../sources");
let query = `
    

SELECT 
    ga4.* EXCEPT(kanaal, sessie_conversie_bron),
    IFNULL(sessie_conversie_bron, kanaal) as kanaal,
    kanaal as kanaal_groep,
    ${ifSource("stg_hubspot_workflowstats" ,`hs_workflow_name,
    edm_name,`)}

FROM(
SELECT
    * EXCEPT(event_ga_session_id, 
        ${ifSource('gs_conversie_mapping', 'conversie_mapping, ')}
        ${ifSource("stg_pivot_targets", "target_soort_conversie,")} 
        ${ifSource('stg_pivot_targets','target_kanaal,')} 
        ${ifSource('stg_pivot_targets','target_merk,')}
        ${ifSource('stg_pivot_targets','target_record_datum,')}
        ${ifSource('stg_pivot_targets','target_account,')}
         kanaal, event_date, ${ifSource("gs_merken", " merk_event,")} account),
    IF(event_name <> "" ${ifSource('gs_ga4_standaard_events', 'AND standaard_event = 0')}, 1, 0) AS conversion_event,
    IF(user_pseudo_id IS NULL AND CAST(event_ga_session_id AS STRING) IS NULL AND event_name <> "" ${ifSource('gs_ga4_standaard_events', 'AND standaard_event = 0')}, unique_event_id, NULL) as privacy_conversion_id, 
    ${ifNull([
        ifSource("gs_conversie_mapping", "conversie_mapping"),
        ifSource("gs_ga4_standaard_events", `IF(standaard_event = 0, event_name, NULL)`),
        ifSource("stg_pivot_targets", "target_soort_conversie")
    ])} as soort_conversie,
    ${ifNull([
        "CAST(kanaal as string)",
        ifSource("stg_pivot_targets", "cast(target_kanaal as string)"),    
    ])} as kanaal,
    ${ifNull([
        ifSource("gs_merken", "CAST(merk_event as string)"),
        ifSource("stg_pivot_targets", "cast(target_merk as string)")    
    ], "as merk_event,")}
    ${ifNull([
        "event_date",
        ifSource("stg_pivot_targets", "target_record_datum"),
    ])} as event_date,
    ${ifNull([
        "account",
        ifSource("stg_pivot_targets", "target_account"),
    ])} as account,
    CASE
        WHEN regexp_contains(session_source,'dv360') 
        OR regexp_contains(session_medium,'^(.*cpm.*)$') THEN 'DV360'
        WHEN regexp_contains(session_source,'facebook|Facebook|fb|instagram|ig|meta')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|.*paid.*)$') THEN 'META'
        WHEN regexp_contains(session_source,'linkedin')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'LinkedIn'
        WHEN regexp_contains(session_source,'google|adwords')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Google Ads'
        WHEN regexp_contains(session_source,'bing')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Microsoft Ads'
        WHEN regexp_contains(session_source,'ActiveCampaign') THEN 'ActiveCampaign'
        WHEN regexp_contains(LOWER(session_medium),'whatsapp') THEN 'Whatsapp'
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
      ${ifSource('gs_conversie_mapping',"gs_mapping.conversie_mapping,")}
      --${ifSource('gs_conversie_mapping','gs_mapping.telmethode as conversie_telmethode,')}
      ${ifSource('gs_conversie_mapping','gs_mapping.softhard as conversie_soft_hard, ')}
      ${ifSource('stg_pivot_targets','targets.account as target_account,')}
      ${ifSource('stg_pivot_targets','targets.soort_conversie as target_soort_conversie,')}
      ${ifSource('stg_pivot_targets','targets.merk as target_merk,')}
      ${ifSource('stg_pivot_targets','targets.kanaal as target_kanaal,')}
      ${ifSource('stg_pivot_targets', 'targets.record_datum as target_record_datum,')}
      ${ifSource('stg_pivot_targets', 'targets.day_target AS conversie_target,')}
    
    FROM ${ref("df_staging_tables", "stg_ga4_events_sessies")} events_sessies
    
    ${join("left join","df_googlesheets_tables","gs_ga4_standaard_events", "AS standaard_event ON TRIM(events_sessies.event_name) = TRIM(standaard_event.event_name) AND events_sessies.account = standaard_event.account")}
    ${join("left join","df_googlesheets_tables","gs_conversie_mapping", "AS gs_mapping ON TRIM(events_sessies.event_name) = TRIM(gs_mapping.event_name) AND events_sessies.account = gs_mapping.account")}
    ${join("full Outer Join","df_staging_views", "stg_pivot_targets", "AS targets ON 1=0")}
  )
) ga4 
    ${join(`LEFT JOIN (SELECT
MAX(hs_workflow_name) AS hs_workflow_name,
MAX(edm_name) AS edm_name,
hs_email_campaignId
FROM`,"stg_hubspot_workflowstats", "AS hs GROUP BY hs_email_campaignId) ON session_content = hs_email_campaignId")}


`

let refs = getRefs() // wordt in dataform gebruikt voor dependency tracking
module.exports = {query, refs}
