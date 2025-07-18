const fs = require('fs');
const path = require('path');

function loadTable(dirPath, schema, type, pk) {
    const exportsMap = {};

    fs.readdirSync(dirPath).forEach(file => {
        const fullPath = path.join(dirPath, file);
        if (
            fs.lstatSync(fullPath).isFile() &&
            file.endsWith('.js') &&
            file !== 'index.js'
        ) {
            const moduleName = path.basename(file, '.js');
            exportsMap[moduleName] = function () {
                const def = require(path.join(dirPath, file));
                const table = {
                    name: moduleName,
                    config: {
                        type: type,
                        schema: schema,
                        dependencies: def.refs
                    },
                    query: def.query
                };
                pk.addSource(table);
                return table;
            };
        }
    });

    return exportsMap;
}

module.exports = loadTable;