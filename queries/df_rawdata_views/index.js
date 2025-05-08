let pk = require('../../sources');
let sources = pk.getSources();

function ga4_events_query() {
    let ga4_select_query = " ( SELECT * FROM "
    let sourceCount = 0;
    for (let s in sources) {
        //for each data source
        if (sources[s].alias === "GA4") {
            if(sourceCount > 0){
                ga4_select_query += " UNION ALL SELECT * FROM "
            }
            ga4_select_query += "`" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "
            sourceCount++;
        }
    }
    ga4_select_query += ")"

    let query = require('./ga4_events.js');
    return query.replace('GA4_BRON', ga4_select_query);
}

function ga4_events(){
    let table = {
        "name": "ga4_events",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": ["events_*"]
        },
        "query": ga4_events_query
    }
    pk.addSource(table);
    return table;
}

module.exports = { ga4_events }