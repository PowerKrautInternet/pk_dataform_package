const loadTables = require('../../utils');
const pk = require('../../sources');

module.exports = loadTables(__dirname, 'df_rawdata_views', 'view', pk);