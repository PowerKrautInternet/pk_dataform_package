const loadTables = require('../../utils/loadTables');
const pk = require('../../sources');

module.exports = loadTables(__dirname, 'df_datamart_views', 'view', pk);