// test/lookupFunctionTests.js
//framework ?

global.dataform = {
    projectConfig: {
        defaultDatabase: process.env.DATAFORM_DEFAULT_DB || "test_db"
    }
};

const assert = require("assert");
const { setupFunctions, function_config } = require("../setup");

const sources = [
    { name: "DummyDataProducer", schema: "rawdata" },
    { name: "MySheet", schema: "googleSheets" },
];

const result = setupFunctions(sources);

// Basischecks
assert.ok(Array.isArray(result), "setupFunctions moet een array teruggeven");
assert.ok(result.length > 0, "Er moeten queries terugkomen");
// Er moet ergens een CREATE FUNCTION tussen zitten
assert.ok(
    result.some(q => typeof q === "string" && q.includes("CREATE OR REPLACE FUNCTION")),
    "Verwacht ten minste één CREATE OR REPLACE FUNCTION query"
);
// Alle items moeten strings zijn
assert.ok(
    result.every(q => typeof q === "string"),
    "Alle queries moeten strings zijn"
);

// lookupTable is nu een SQL UDF, niet meer JS.
const lookup_ddl = result.find(q => typeof q === "string" && q.includes("lookupTable"));
assert.ok(lookup_ddl, "Verwacht een DDL voor lookupTable");
assert.ok(
    lookup_ddl.includes("RETURNS STRING") && lookup_ddl.includes(" AS ( "),
    "lookupTable moet een SQL UDF zijn (RETURNS STRING AS ( ... ))"
);
assert.ok(
    !lookup_ddl.includes("LANGUAGE js"),
    "lookupTable mag geen JavaScript UDF meer zijn"
);
assert.ok(
    lookup_ddl.includes("REGEXP_EXTRACT") &&
    lookup_ddl.includes("STRING_AGG") &&
    lookup_ddl.includes("JSON_VALUE_ARRAY") &&
    lookup_ddl.includes("NORMALIZE_AND_CASEFOLD"),
    "lookupTable SQL body mist verwachte BigQuery-functies"
);
assert.ok(
    lookup_ddl.includes("ORDER BY CHAR_LENGTH(opt) DESC"),
    "lookupTable moet langste-merk-eerst prioriteit hanteren"
);

console.log("✓ implementation tests passed");
