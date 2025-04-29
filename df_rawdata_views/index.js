let sources = require('../index')

function ga4_events_query() {
    let ga4_select_query = " ( SELECT * FROM "
    let sourceCount = 0;
    for (let s in sources) {
        //for each data source
        if (sources[s].type == "GA4") {
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
    return {
        "name": "ga4_events",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views"
        },
        "query": ga4_events_query()
    }
}

module.exports (ga4_events())