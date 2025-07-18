let pk = require('../../sources');
let sources = pk.getSources();

function dm_hubspot_emailstats(){
    let table = {
        "name": "dm_hubspot_emailstats",
        "config": {
            "type": "view",
            "schema": "df_datamart_views",
            "dependencies": require('./dm_hubspot_emailstats').refs
        },
        "query": require('./dm_hubspot_emailstats').query
    }
    pk.addSource(table);
    return table;
}

module.exports = {
    dm_hubspot_emailstats
}