const pk = require('../sources');

function exportTables(defaultType, schema, definitions) {
    const output = {};

    definitions.forEach(entry => {
        let name, type, extraConfig;

        if (typeof entry === 'string') {
            name = entry;
            type = defaultType;
            extraConfig = {};
        } else if (typeof entry === 'object') {
            name = entry.name;
            type = entry.type || defaultType;
            extraConfig = { ...entry };
            delete extraConfig.name;
            delete extraConfig.type;
        }

        output[name] = () => {
            const mod = require(`./${name}`);
            const table = {
                name,
                config: {
                    type,
                    schema,
                    dependencies: mod.refs,
                    ...extraConfig
                },
                query: mod.query
            };
            pk.addSource(table);
            return table;
        };
    });

    return output;
}

module.exports = exportTables;
