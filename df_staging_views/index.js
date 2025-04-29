let pk = require('../index.js');
let sources = pk.getSources();

function stg_ga4_events_sessies () {
    return require('./stg_ga4_events_sessies')
}

function stg_ga4_events_sessies_query () {
    let table = {
        "name": "ga4_events_sessies",
        "config": {
            "type": "view",
            "schema": "df_staging_views"
        },
        "query": stg_ga4_events_sessies
    }
    pk.addSource(table);
    return table;
}

module.exports = { stg_ga4_events_sessies, stg_ga4_events_sessies_query }