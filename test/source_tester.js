let test_db = 'pk-datalake-test'

const sources = [
    {schema: "rawdata", name: "bingAdsDataProducer"},
    {schema: "rawdata", name: "facebookDataProducer"},
    {schema: "rawdata", name: "linkedInAdsDataProducer"},
    {schema: "googleAds", name: "ads_AdGroupBasicStats_3008480939"},
    {schema: "googleAds", name: "ads_AdGroupConversionStats_3008480939"},
    {schema: "googleAds", name: "ads_AdGroup_3008480939"},
    {schema: "googleAds", name: "ads_CampaignBasicStats_3008480939"},
    {schema: "googleAds", name: "ads_CampaignConversionStats_3008480939"},
    {schema: "googleAds", name: "ads_Campaign_3008480939"},
    {schema: "analytics_1", name: "events_*"},
    {schema: "analytics_2", name: "events_*"},
    {schema: "googleSheets", name: "gs_conversie_targets"},
    {schema: "googleSheets", name: "gs_conversie_mapping"},
    {schema: "googleSheets", name: "gs_conversie_mapping_other", alias: "gs_conversie_mapping"},
]

const source_helper = require("../helper_functions/source_helper")
const source = new source_helper(test_db);
source.setSources(sources);

//test 1: enkele ref
if (source.ref({name: "gs_conversie_targets"}) !== `\`${test_db}.googleSheets.gs_conversie_targets\``) {
    throw new Error("Test 1: failed")
} else {
    console.log("Test 1: succesfull")
}

//test 2: meerdere resultaten in ref
let verwacht_resultaat =
`(SELECT * FROM \`${test_db}.analytics_1.events_\` 
UNION ALL 
SELECT * FROM \`${test_db}.analytics_2.events_\`)`;
if (source.ref({name: "events_"}) !== verwacht_resultaat) {
    throw new Error("Test 2: failed")
} else {
    console.log("Test 2: succesfull")
}

//test 3: meerdere resultaten in ref with alias
let verwacht_resultaat2 =
`(SELECT * FROM \`${test_db}.googleSheets.gs_conversie_mapping\` 
UNION ALL 
SELECT * FROM \`${test_db}.googleSheets.gs_conversie_mapping_other\`)`;
if (source.ref({name: "events_"}) !== verwacht_resultaat2) {
    throw new Error("Test 3: failed")
} else {
    console.log("Test 3: succesfull")
}