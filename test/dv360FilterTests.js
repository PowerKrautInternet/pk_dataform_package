global.dataform = {
    projectConfig: {
        defaultDatabase: "test-db",
        defaultSchema: "def",
        schemaSuffix: ""
    }
};

const s = require("../sources");

function reset(sources) {
    s.setSources(sources);
}

function assertContains(label, output, needle) {
    if (output.indexOf(needle) === -1) {
        console.error("FAIL [" + label + "] expected to contain: " + needle);
        console.error("---OUTPUT---\n" + output + "\n---");
        process.exit(1);
    } else {
        console.log("PASS [" + label + "] contains: " + needle);
    }
}

function assertNotContains(label, output, needle) {
    if (output.indexOf(needle) !== -1) {
        console.error("FAIL [" + label + "] expected NOT to contain: " + needle);
        console.error("---OUTPUT---\n" + output + "\n---");
        process.exit(1);
    } else {
        console.log("PASS [" + label + "] does not contain: " + needle);
    }
}

// 1. String advertiser_id
reset([{
    database: "pk-datalake-powerkraut",
    schema: "rawdata",
    name: "dv360_central",
    alias: "DV360",
    account: "AutoXYZ",
    advertiser_id: "123456"
}]);
let out = s.ref("DV360");
assertContains("string-id", out, "WHERE advertiser_id IN ('123456')");
assertContains("string-id", out, "`pk-datalake-powerkraut.rawdata.dv360_central`");

// 2. Array advertiser_id with quote-escape
reset([{
    database: "pk-datalake-powerkraut",
    schema: "rawdata",
    name: "dv360_central",
    alias: "DV360",
    advertiser_id: ["123456", "78'9012"]
}]);
out = s.ref("DV360");
assertContains("array-id", out, "WHERE advertiser_id IN ('123456','78''9012')");

// 3. No advertiser_id — no WHERE injected
reset([{
    database: "pk-datalake-powerkraut",
    schema: "rawdata",
    name: "dv360_central",
    alias: "DV360"
}]);
out = s.ref("DV360");
assertNotContains("no-id", out, "WHERE advertiser_id");

// 4. Non-DV360 source with advertiser_id — ignored
reset([{
    database: "some-db",
    schema: "df_rawdata_views",
    name: "ga4_events",
    alias: "ga4_events",
    advertiser_id: "999"
}]);
out = s.ref("ga4_events");
assertNotContains("non-dv360", out, "WHERE advertiser_id");

// 5. Multiple DV360 declarations (UNION ALL) — each arm gets own filter
reset([
    { database: "pk-datalake-powerkraut", schema: "rawdata", name: "dv360_central_a", alias: "DV360", advertiser_id: "111" },
    { database: "pk-datalake-powerkraut", schema: "rawdata", name: "dv360_central_b", alias: "DV360", advertiser_id: "222" }
]);
out = s.ref("DV360");
assertContains("multi-arm", out, "WHERE advertiser_id IN ('111')");
assertContains("multi-arm", out, "WHERE advertiser_id IN ('222')");
assertContains("multi-arm", out, "UNION ALL");

// 6. Number advertiser_id — bare numeric (geen quotes), nodig voor INT64-kolommen
reset([{
    database: "pk-datalake-powerkraut",
    schema: "DV360_marketingdashboard",
    name: "dv360_eenmalige_export_20250101_20260607",
    alias: "DV360",
    account: "Apoint",
    advertiser_id: 1285486742
}]);
out = s.ref("DV360");
assertContains("number-id", out, "WHERE advertiser_id IN (1285486742)");
assertNotContains("number-id-no-quotes", out, "'1285486742'");

// 7. Array van numbers
reset([{
    database: "pk-datalake-powerkraut",
    schema: "DV360_marketingdashboard",
    name: "dv360_eenmalige_export_20250101_20260607",
    alias: "DV360",
    advertiser_id: [6950824531, 953561454, 6999030610]
}]);
out = s.ref("DV360");
assertContains("array-numbers", out, "WHERE advertiser_id IN (6950824531,953561454,6999030610)");

console.log("\nAll smoke tests passed.");
