let pk = require('../index.js');

function stg_ga4_events_sessies () {
    let table = {
        "name": "stg_ga4_events_sessies",
        "config": {
            "type": "table",
            "schema": "df_staging_tables",
            "dependencies": require('./stg_ga4_events_sessies').refs,
            "uniqueKey": ["unique_event_id"],
            "bigquery": {
                "partitionBy": "event_date",
                "updatePartitionFilter":
                    "event_date >= date_sub(current_date(), interval 1 day)"
            }
        },
        "query": require('./stg_ga4_events_sessies').query
    }
    pk.addSource(table);
    return table;
}

module.exports = { stg_ga4_events_sessies }