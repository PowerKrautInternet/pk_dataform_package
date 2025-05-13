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

function syntec_leads () {
    let table = {
        "name": "syntec_leads",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./syntec_leads').refs
        },
        "query": require('./syntec_leads').query
    }
    pk.addSource(table);
    return table;
}

function syntec_orders () {
    let table = {
        "name": "syntec_orders",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./syntec_orders').refs
        },
        "query": require('./syntec_orders').query
    }
    pk.addSource(table);
    return table;
}

function dv360_data () {
    let table = {
        "name": "dv360_data",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./dv360_data').refs
        },
        "query": require('./dv360_data').query
    }
    pk.addSource(table);
    return table;
}

function activecampagin_edm () {
    let table = {
        "name": "activecampagin_edm",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./activecampagin_edm').refs
        },
        "query": require('./activecampagin_edm').query
    }
    pk.addSource(table);
    return table;
}

function activecampaign_workflows () {
    let table = {
        "name": "activecampaign_workflows",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./activecampaign_workflows').refs
        },
        "query": require('./activecampaign_workflows').query
    }
    pk.addSource(table);
    return table;
}

function bing_ad_group_performance () {
    let table = {
        "name": "bing_ad_group_performance",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./bing_ad_group_performance').refs
        },
        "query": require('./bing_ad_group_performance').query
    }
    pk.addSource(table);
    return table;
}

module.exports = { 
    ga4_events,
    syntec_orders,
    syntec_leads,
    dv360_data,
    activecampagin_edm,
    activecampaign_workflows,
    bing_ad_group_performance
}