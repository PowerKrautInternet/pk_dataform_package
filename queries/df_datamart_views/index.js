const exportTables = require('../../utils');

module.exports.loadTables = exportTables(
    "view", "df_datamart_views",
    ["dm_hubspot_emailstats"]
);