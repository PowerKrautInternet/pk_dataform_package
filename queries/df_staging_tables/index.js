const exportTables = require('../../utils');

module.exports.loadTables = exportTables(
    "table", "df_staging_tables", [
        {
            name: "stg_ga4_events_sessies",
            bigquery: {
                partitionBy: "event_date",
                updatePartitionFilter: "event_date >= date_sub(current_date(), interval 1 day)"
            }
        },
        {name: "stg_hubspot_contacts_count", type: "incremental"}
    ]
);