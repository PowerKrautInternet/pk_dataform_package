/*config*/
let pk = require("../../sources")
let sources = pk.getSources().map((s) => s.alias ?? s.name )
let ref = pk.ref
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
    mapping_thema AS flow_thema,
    "ActiveCampaign" AS bron,
    "ActiveCampaign" AS kanaal,
    aantal AS aantal_contacts
FROM
${ref("df_staging_views", "stg_activecampaign_workflow_edm")} ac

`; if(sources.includes("gs_activecampaign_ga4_mapping")) {query += `    //BEGIN gs_activecampaign_ga4_mapping

LEFT JOIN
(
    SELECT
        mapping_edm,
        mapping_flows,
        mapping_thema
    FROM
        ${ref("gs_activecampaign_ga4_mapping")}
    GROUP BY
        mapping_edm,
        mapping_flows,
        mapping_thema
) campaign
ON
    ac.campaign_name = campaign.mapping_edm
AND 
    ac.name = campaign.mapping_flows
    
`}query += `                                                            //END gs_activecampaign_ga4_mapping

FULL OUTER JOIN
    ${ref("gs_activecampaign_totalcontacts")} contacts
ON
    1=0
    
    `
let refs = pk.getRefs()
module.exports = {query, refs}