const exportTables = require('../../utils');

module.exports.loadTables = exportTables(
    "view", "df_staging_views", [
        "stg_activecampaign_ga4_sheets",
        "stg_activecampaign_workflow_edm",
        "stg_ga4_events_sessies",
        "stg_ga4_sessie_assignment",
        "stg_pivot_targets",
        "stg_marketingkanalen_combined",
        "stg_marketingdashboard_searchconsole",
        "stg_syntec_leads_orders_combined",
        "stg_ga4_marketing_kanalen_combined",
        "stg_ga4_mappings_targets",
        "stg_google_ads_adgroup_combined",
        "stg_facebookdata",
        "stg_bing_ad_group_performance",
        "stg_linkedin_ads_combined",
        "stg_lef_leads_agg",
        "stg_googleads_combined",
        "stg_googleads_perfmax_combined",
        "stg_hubspot_emailevents_campaigns",
        "stg_hubspot_workflowstats"
    ]
);