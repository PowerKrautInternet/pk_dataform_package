const exportTables = require('../../utils');

module.exports.loadTables = exportTables(
    "view", "df_rawdata_views", [
        "activecampaign_edm",
        "activecampaign_workflows",
        "bing_ad_group_performance",
        "bing_assetgroup_performance",
        "dv360_data",
        "facebookdata",
        "ga4_events",
        "hubspot_bigquerylogging",
        "hubspot_emailcampaigns",
        "hubspot_emailevents",
        "lef_leads",
        "linkedin_ad_account",
        "linkedin_ad_campagin_group",
        "linkedin_ads_analytics",
        "linkedin_campagin_information",
        "syntec_leads",
        "syntec_orders"
    ]
);