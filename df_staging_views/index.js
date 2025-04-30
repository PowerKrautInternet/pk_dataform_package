let pk = require('../index.js');

function stg_ga4_events_sessies () {
    return require('./stg_ga4_events_sessies')
}

function stg_ga4_events_sessies_query () {
    let table = {
        "name": "stg_ga4_events_sessies",
        "config": {
            "type": "view",
            "schema": "df_staging_views"
        },
        "query": stg_ga4_events_sessies
    }
    pk.addSource(table);
    return table;
}

function stg_ga4_mappings_targets () {
    let table = {
        "name": "stg_ga4_mappings_targets",
        "config": {
            "type": "view",
            "schema": "df_staging_views"
        },
        "query": require('./stg_ga4_mappings_targets')
    }
    pk.addSource(table);
    return table;
}

module.exports = { stg_ga4_mappings_targets, stg_ga4_events_sessies_query }