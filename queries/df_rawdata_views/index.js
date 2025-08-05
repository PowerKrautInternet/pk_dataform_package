let pk = require('../../sources');
let sources = pk.getSources();

function ga4_events(){
    let table = {
        "name": "ga4_events",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./ga4_events').refs
        },
        "query": require('./ga4_events').query
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

function activecampaign_edm () {
    let table = {
        "name": "activecampaign_edm",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./activecampaign_edm').refs
        },
        "query": require('./activecampaign_edm').query
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

function bing_assetgroup_performance () {
    let table = {
        "name": "bing_assetgroup_performance",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./bing_assetgroup_performance').refs
        },
        "query": require('./bing_assetgroup_performance').query
    }
    pk.addSource(table);
    return table;
}

function facebookdata() {
    let table = {
        "name": "facebookdata",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./facebookdata').refs
        },
        "query": require('./facebookdata').query
    }
    pk.addSource(table);
    return table;
}

function linkedin_ads_analytics(){
    let table = {
        "name": "linkedin_ads_analytics",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./linkedin_ads_analytics').refs
        },
        "query": require('./linkedin_ads_analytics').query
    }
    pk.addSource(table);
    return table;
}

function linkedin_ad_account() {
    let table = {
        "name": "linkedin_ad_account",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./linkedin_ad_account').refs
        },
        "query": require('./linkedin_ad_account').query
    }
    pk.addSource(table);
    return table;
}

function linkedin_campaign_information () {
    let table = {
        "name": "linkedin_campaign_information",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./linkedin_campaign_information').refs
        },
        "query": require("./linkedin_campaign_information").query
    }
    pk.addSource(table);
    return table;
}

function linkedin_ad_campaign_group() {
    let table = {
        "name": "linkedin_ad_campaign_group",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./linkedin_ad_campaign_group').refs
        },
        "query": require('./linkedin_ad_campaign_group').query
    }
    pk.addSource(table);
    return table;
}

function lef_leads() {
    let table = {
        "name": "lef_leads",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./lef_leads').refs
        },
        "query": require('./lef_leads').query
    }
    pk.addSource(table);
    return table;
}

module.exports = { 
    ga4_events,
    syntec_orders,
    syntec_leads,
    dv360_data,
    activecampaign_edm,
    activecampaign_workflows,
    bing_ad_group_performance,
    bing_assetgroup_performance,
    facebookdata,
    linkedin_ads_analytics,
    linkedin_ad_account,
    linkedin_campaign_information,
    linkedin_ad_campaign_group,
    lef_leads
}
