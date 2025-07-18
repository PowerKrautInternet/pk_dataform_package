const fs = require('fs');
const path = require('path');
const pk = require('../../sources');

const dirPath = __dirname; // huidige directory (waar dit bestand staat)
const exportsMap = {};

// Lees alle .js-bestanden uit de map
fs.readdirSync(dirPath).forEach(file => {
    const fullPath = path.join(dirPath, file);
    if (fs.lstatSync(fullPath).isFile() && file.endsWith('.js') && file !== 'index.js') {
        const moduleName = path.basename(file, '.js');
        exportsMap[moduleName] = function () {
            const def = require(`./${moduleName}`);
            const table = {
                name: moduleName,
                config: {
                    type: 'view',
                    schema: 'df_staging_views',
                    dependencies: def.refs
                },
                query: def.query
            };
            pk.addSource(table);
            return table;
        };
    }
});

module.exports = exportsMap;
