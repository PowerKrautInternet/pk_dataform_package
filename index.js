function table (publishFunction, endTable, config, query) {
    publishFunction(endTable, config).query(query)
}

module.exports = {table}