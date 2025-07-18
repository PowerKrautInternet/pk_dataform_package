const loadTables = require('../../utils/loadTables');
const pk = require('../../sources');

module.exports = loadTables(__dirname, 'df_rawdata_views', 'view', pk);