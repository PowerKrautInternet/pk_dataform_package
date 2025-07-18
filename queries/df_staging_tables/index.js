let pk = require('../../sources');

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

function stg_hubspot_contacts_count () {
    let table = {
        "name": "stg_ga4_events_sessies",
        "config": {
            "type": "incremental",
            "schema": "df_staging_tables",
            "dependencies": require('./stg_hubspot_contacts_count').refs,
        },
        "query": require('./stg_hubspot_contacts_count').query
    }
    pk.addSource(table);
    return table;
}

module.exports = { stg_ga4_events_sessies, stg_hubspot_contacts_count }