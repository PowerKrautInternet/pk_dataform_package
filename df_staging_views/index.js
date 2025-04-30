let pk = require('../index.js');

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
            "dependencies": require('./stg_ga4_events_sessies').refs
        },
        "query": require('./stg_ga4_events_sessies').query
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
            "dependencies": require('./stg_ga4_events_sessies').refs
        },
        "query": require('./stg_ga4_events_sessies').query
    }
    pk.addSource(table);
    return table;
}

module.exports = { stg_ga4_mappings_targets, stg_ga4_events_sessies, stg_ga4_sessie_assignment, stg_pivot_targets }