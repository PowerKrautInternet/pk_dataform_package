let pk = require('../../sources');

function stg_ga4_events_sessies () {
    let table = {
        "name": "stg_ga4_events_sessies",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_ga4_events_sessies').refs
        },
        "query": require('./stg_ga4_events_sessies').query
    }
    pk.addSource(table);
    return table;
}

function stg_ga4_sessie_assignment () {
    let table = {
        "name": "stg_ga4_sessie_assignment",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_ga4_sessie_assignment').refs
        },
        "query": require('./stg_ga4_sessie_assignment').query
    }
    pk.addSource(table);
    return table;
}

function stg_pivot_targets () {
    let table = {
        "name": "stg_pivot_targets",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_pivot_targets').refs
        },
        "query": require('./stg_pivot_targets').query
    }
    pk.addSource(table);
    return table;
}

function stg_marketingkanalen_combined () {
    let table = {
        "name": "stg_marketingkanalen_combined",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_marketingkanalen_combined').refs
        },
        "query": require('./stg_marketingkanalen_combined').query
    }
    pk.addSource(table);
    return table;
}

function stg_syntec_leads_orders_combined () {
    let table = {
        "name": "stg_syntec_leads_orders_combined",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_syntec_leads_orders_combined').refs
        },
        "query": require('./stg_syntec_leads_orders_combined').query
    }
    pk.addSource(table);
    return table;
}

function stg_activecampaign_ga4_sheets () {
    let table = {
        "name": "stg_activecampaign_ga4_sheets",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_activecampaign_ga4_sheets').refs
        },
        "query": require('./stg_activecampaign_ga4_sheets').query
    }
    pk.addSource(table);
    return table;
}

function stg_ga4_marketing_kanalen_combined () {
    let table = {
        "name": "stg_ga4_marketing_kanalen_combined",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_ga4_marketing_kanalen_combined').refs
        },
        "query": require('./stg_ga4_marketing_kanalen_combined').query
    }
    pk.addSource(table);
    return table;
}

function stg_ga4_mappings_targets(){
    let table = {
        "name": "stg_ga4_mappings_targets",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_ga4_mappings_targets').refs
        },
        "query": require('./stg_ga4_mappings_targets').query
    }
    pk.addSource(table);
    return table;
}

function stg_marketingdashboard_searchconsole() {
    let table = {
        "name": "stg_marketingdashboard_searchconsole",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_marketingdashboard_searchconsole').refs
        },
        "query": require('./stg_marketingdashboard_searchconsole').query
    }
    pk.addSource(table);
    return table;
}

function stg_google_ads_adgroup_combined() {
    let table = {
        "name": "stg_google_ads_adgroup_combined",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_google_ads_adgroup_combined').refs
        },
        "query": require('./stg_google_ads_adgroup_combined').query
    }
    pk.addSource(table);
    return table;
}

function stg_facebookdata () {
    let table = {
        "name": "stg_facebookdata",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_facebookdata').refs
        },
        "query": require('./stg_facebookdata').query
    }
    pk.addSource(table);
    return table;
}

function stg_bing_ad_group_performance () {
    let table = {
        "name": "stg_bing_ad_group_performance",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_bing_ad_group_performance').refs
        },
        "query": require('./stg_bing_ad_group_performance').query
    }
    pk.addSource(table);
    return table;
}

function stg_linkedin_ads_combined () {
    let table = {
        "name": "stg_linkedin_ads_combined",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_linkedin_ads_combined').refs
        },
        "query": require('./stg_linkedin_ads_combined').query
    }
    pk.addSource(table);
    return table;
}

function stg_activecampaign_workflow_edm() {
    let table = {
        "name": "stg_activecampaign_workflow_edm",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_activecampaign_workflow_edm').refs
        },
        "query": require('./stg_activecampaign_workflow_edm').query
    }
    pk.addSource(table);
    return table;
}

function stg_lef_leads_agg(){
    let table = {
        "name": "stg_lef_leads_agg",
        "config": {
            "type": "view",
            "schema": "df_staging_views",
            "dependencies": require('./stg_lef_leads_agg').refs
        },
        "query": require('./stg_lef_leads_agg').query
    }
}

module.exports = {
    stg_ga4_events_sessies,
    stg_ga4_sessie_assignment,
    stg_pivot_targets,
    stg_marketingkanalen_combined,
    stg_marketingdashboard_searchconsole,
    stg_syntec_leads_orders_combined,
    stg_activecampaign_ga4_sheets,
    stg_ga4_marketing_kanalen_combined,
    stg_ga4_mappings_targets,
    stg_google_ads_adgroup_combined,
    stg_facebookdata,
    stg_bing_ad_group_performance,
    stg_linkedin_ads_combined,
    stg_activecampaign_workflow_edm,
    stg_lef_leads_agg
}