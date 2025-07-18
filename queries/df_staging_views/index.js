const loadTables = require('../../utils');
const pk = require('../../sources');

module.exports = loadTables(__dirname, 'df_staging_views', 'view', pk);