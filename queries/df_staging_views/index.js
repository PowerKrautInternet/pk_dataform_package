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

function stg_ga4_mappings_targets () {
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

function stg_marketingdashboard_searchconsole () {
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

module.exports = {
    stg_ga4_mappings_targets,
    stg_ga4_events_sessies,
    stg_ga4_sessie_assignment,
    stg_pivot_targets,
    stg_marketingkanalen_combined,
    stg_marketingdashboard_searchconsole,
    stg_syntec_leads_orders_combined,
    stg_activecampaign_ga4_sheets
}