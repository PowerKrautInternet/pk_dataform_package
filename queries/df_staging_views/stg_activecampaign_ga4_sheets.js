/*config*/
const {ifSource, join, ref, getRefs, getSources} = require("../../sources");
let sources = getSources().map((s) => s.alias ?? s.name )
let query = `

SELECT
IFNULL(campaign_date, date) AS record_datum,
    name AS ac_name,
    campaign_name,
    subject_name AS ac_subject_name,
    contacts_entered,
    flow_campaigns,
    total_sends,
    unique_opens,
    total_clicks,
    unsubscribes,
    total_bounces,
    open_rate,
    click_rate,
    unsubscribe_rate,
    forward_rate,
    bounce_rates,
    click_to_open_ratio,
    workflow_status,
    IF(name <> "", "Workflows", "eDM") AS ac_bron,
    ${ifSource("gs_activecampaign_ga4_mapping", "mapping_thema AS flow_thema,")}
    "ActiveCampaign" AS bron,
    "ActiveCampaign" AS kanaal,
    aantal AS aantal_contacts,
    ac.account,
FROM
${ref("df_staging_views", "stg_activecampaign_workflow_edm")} ac

${join( 
    "LEFT JOIN(SELECT mapping_edm, mapping_flows, mapping_thema FROM", 
    "df_googlesheets_tables",
    "gs_activecampaign_ga4_mapping", 
    "GROUP BY mapping_edm, mapping_flows, mapping_thema) campaign ON ac.campaign_name = campaign.mapping_edm AND ac.name = campaign.mapping_flows"
)}
${join("FULL OUTER JOIN", "df_googlesheets_tables","gs_activecampaign_totalcontacts", "AS contacts ON 1=0")}
    
    `
let refs = getRefs()
module.exports = {query, refs}