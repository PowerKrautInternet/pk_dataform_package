let pk = require('../index.js');

function stg_ga4_events_sessies () {
    let table = {
        "name": "stg_ga4_events_sessies",
        "config": {
            "type": "table",
            "schema": "df_staging_tables"
        },
        "query": require('./stg_ga4_events_sessies')
    }
    pk.addSource(table);
    return table;
}

module.exports = { stg_ga4_events_sessies }