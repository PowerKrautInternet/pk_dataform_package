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

function vistar_media_ads () {
    let table = {
        "name": "vistar_media_ads",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./vistar_media_ads').refs
        },
        "query": require('./vistar_media_ads').query
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

function hubspot_emailcampaigns () {
    let table = {
        "name": "hubspot_emailcampaigns",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./hubspot_emailcampaigns').refs
        },
        "query": require("./hubspot_emailcampaigns").query
    }
    pk.addSource(table);
    return table;
}

function hubspot_bigquerylogging() {
    let table = {
        "name": "hubspot_bigquerylogging",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./hubspot_bigquerylogging').refs
        },
        "query": require('./hubspot_bigquerylogging').query
    }
    pk.addSource(table);
    return table;
}

function hubspot_emailevents() {
    let table = {
        "name": "hubspot_emailevents",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./hubspot_emailevents').refs
        },
        "query": require('./hubspot_emailevents').query
    }
    pk.addSource(table);
    return table;
}

function googleads_campaignlabel() {
    let table = {
        "name": "googleads_campaignlabel",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./googleads_campaignlabel').refs
        },
        "query": require('./googleads_campaignlabel').query
    }
    pk.addSource(table);
    return table;
}

function sam_adres() {
    let table = {
        "name": "sam_adres",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_adres').refs
        },
        "query": require('./sam_adres').query
    }
    pk.addSource(table);
    return table;
}


function sam_aflever_trajecten() {
    let table = {
        "name": "sam_aflever_trajecten",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_aflever_trajecten').refs
        },
        "query": require('./sam_aflever_trajecten').query
    }
    pk.addSource(table);
    return table;
}

function sam_aflevering() {
    let table = {
        "name": "sam_aflevering",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_aflevering').refs
        },
        "query": require('./sam_aflevering').query
    }
    pk.addSource(table);
    return table;
}

function sam_aflevering_model() {
    let table = {
        "name": "sam_aflevering_model",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_aflevering_model').refs
        },
        "query": require('./sam_aflevering_model').query
    }
    pk.addSource(table);
    return table;
}

function sam_afsluitreden() {
    let table = {
        "name": "sam_afsluitreden",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_afsluitreden').refs
        },
        "query": require('./sam_afsluitreden').query
    }
    pk.addSource(table);
    return table;
}

function sam_automerk() {
    let table = {
        "name": "sam_automerk",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_automerk').refs
        },
        "query": require('./sam_automerk').query
    }
    pk.addSource(table);
    return table;
}

function sam_dealer() {
    let table = {
        "name": "sam_dealer",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_dealer').refs
        },
        "query": require('./sam_dealer').query
    }
    pk.addSource(table);
    return table;
}

function sam_herkomst() {
    let table = {
        "name": "sam_herkomst",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_herkomst').refs
        },
        "query": require('./sam_herkomst').query
    }
    pk.addSource(table);
    return table;
}

function sam_occasion() {
    let table = {
        "name": "sam_occasion",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_occasion').refs
        },
        "query": require('./sam_occasion').query
    }
    pk.addSource(table);
    return table;
}

function sam_offerte_status() {
    let table = {
        "name": "sam_offerte_status",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_offerte_status').refs
        },
        "query": require('./sam_offerte_status').query
    }
    pk.addSource(table);
    return table;
}

function sam_offerte_vtr() {
    let table = {
        "name": "sam_offerte_vtr",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_offerte_vtr').refs
        },
        "query": require('./sam_offerte_vtr').query
    }
    pk.addSource(table);
    return table;
}

function sam_offertes() {
    let table = {
        "name": "sam_offertes",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_offertes').refs
        },
        "query": require('./sam_offertes').query
    }
    pk.addSource(table);
    return table;
}

function sam_relatie() {
    let table = {
        "name": "sam_relatie",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_relatie').refs
        },
        "query": require('./sam_relatie').query
    }
    pk.addSource(table);
    return table;
}

function sam_relatieoptin() {
    let table = {
        "name": "sam_relatieoptin",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_relatieoptin').refs
        },
        "query": require('./sam_relatieoptin').query
    }
    pk.addSource(table);
    return table;
}

function sam_relatieoptinsgdpr_pivot() {
    let table = {
        "name": "sam_relatieoptinsgdpr_pivot",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_relatieoptinsgdpr_pivot').refs
        },
        "query": require('./sam_relatieoptinsgdpr_pivot').query
    }
    pk.addSource(table);
    return table;
}

function sam_sales_trajecten() {
    let table = {
        "name": "sam_sales_trajecten",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_sales_trajecten').refs
        },
        "query": require('./sam_sales_trajecten').query
    }
    pk.addSource(table);
    return table;
}

function sam_soortbrandstof() {
    let table = {
        "name": "sam_soortbrandstof",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_soortbrandstof').refs
        },
        "query": require('./sam_soortbrandstof').query
    }
    pk.addSource(table);
    return table;
}

function sam_soortklantcategorie() {
    let table = {
        "name": "sam_soortklantcategorie",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_soortklantcategorie').refs
        },
        "query": require('./sam_soortklantcategorie').query
    }
    pk.addSource(table);
    return table;
}

function sam_table_hashes() {
    let table = {
        "name": "sam_table_hashes",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_table_hashes').refs
        },
        "query": require('./sam_table_hashes').query
    }
    pk.addSource(table);
    return table;
}

function sam_trajectfilterleads() {
    let table = {
        "name": "sam_trajectfilterleads",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_trajectfilterleads').refs
        },
        "query": require('./sam_trajectfilterleads').query
    }
    pk.addSource(table);
    return table;
}

function sam_trajectfilterorders() {
    let table = {
        "name": "sam_trajectfilterorders",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_trajectfilterorders').refs
        },
        "query": require('./sam_trajectfilterorders').query
    }
    pk.addSource(table);
    return table;
}
function sam_trajects() {
    let table = {
        "name": "sam_trajects",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_trajects').refs
        },
        "query": require('./sam_trajects').query
    }
    pk.addSource(table);
    return table;
}

function sam_trajects_def_offerte() {
    let table = {
        "name": "sam_trajects_def_offerte",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_trajects_def_offerte').refs
        },
        "query": require('./sam_trajects_def_offerte').query
    }
    pk.addSource(table);
    return table;
}

function sam_trajects_extern() {
    let table = {
        "name": "sam_trajects_extern",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_trajects_extern').refs
        },
        "query": require('./sam_trajects_extern').query
    }
    pk.addSource(table);
    return table;
}

function sam_verkoper() {
    let table = {
        "name": "sam_verkoper",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./sam_verkoper').refs
        },
        "query": require('./sam_verkoper').query
    }
    pk.addSource(table);
    return table;
}

function taxatiemoduleonline() {
    let table = {
        "name": "taxatiemoduleonline",
        "config": {
            "type": "view",
            "schema": "df_rawdata_views",
            "dependencies": require('./taxatiemoduleonline').refs
        },
        "query": require('./taxatiemoduleonline').query
    }
    pk.addSource(table);
    return table;
}
module.exports = {
    activecampaign_edm,
    ga4_events,
    syntec_orders,
    syntec_leads,
    dv360_data,
    activecampaign_workflows,
    bing_ad_group_performance,
    bing_assetgroup_performance,
    facebookdata,
    linkedin_ads_analytics,
    linkedin_ad_account,
    linkedin_campaign_information,
    linkedin_ad_campaign_group,
    lef_leads,
    hubspot_bigquerylogging,
    hubspot_emailcampaigns,
    hubspot_emailevents,
    googleads_campaignlabel,
    sam_adres,
    sam_aflever_trajecten,
    sam_aflevering,
    sam_aflevering_model,
    sam_afsluitreden,
    sam_automerk,
    sam_dealer,
    sam_herkomst,
    sam_occasion,
    sam_offerte_status,
    sam_offerte_vtr,
    sam_offertes,
    sam_relatie,
    sam_relatieoptin,
    sam_relatieoptinsgdpr_pivot,
    sam_sales_trajecten,
    sam_soortbrandstof,
    sam_soortklantcategorie,
    sam_table_hashes,
    sam_trajectfilterleads,
    sam_trajectfilterorders,
    sam_trajects,
    sam_trajects_def_offerte,
    sam_trajects_extern,
    sam_verkoper,
    taxatiemoduleonline
    
}
