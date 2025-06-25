function table (publishFunction, endTable, config, query) {
    publishFunction(endTable, config).query(query)
}

let config = {type: "view"}

class viewClass {
    constructor () {
        this.select_block = true
        this.from_block = true
        this.viewQuery = {select: "*", from: "`anywhere`"}
        return this
    };
    select (enable) {
        this.select_block = enable
        return this
    }
    query (query) {
        let temp = "";
        temp += this.select_block ? this.viewQuery.select : "";
        temp += this.from_block ? this.viewQuery.from : "";
        return temp

    }
}

function view () {
    return new viewClass()
}

module.exports = {table, config, view}