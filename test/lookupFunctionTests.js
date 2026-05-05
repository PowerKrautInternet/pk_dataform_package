// test/lookupFunctionTests.js
//framework ?

global.dataform = {
    projectConfig: {
        defaultDatabase: process.env.DATAFORM_DEFAULT_DB || "test_db"
    }
};

const assert = require("assert");
const { setupFunctions, function_config } = require("../setup");
const { test_cases } = require("../setup/lookup_function_tests");

const sources = [
    { name: "DummyDataProducer", schema: "rawdata" },
    { name: "MySheet", schema: "googleSheets" },
];

const result = setupFunctions(sources);

// Basischecks
assert.ok(Array.isArray(result), "setupFunctions moet een array teruggeven");
assert.ok(result.length > 0, "Er moeten queries terugkomen");
assert.ok(
    result.some(q => typeof q === "string" && q.includes("CREATE OR REPLACE FUNCTION")),
    "Verwacht ten minste één CREATE OR REPLACE FUNCTION query"
);
assert.ok(
    result.every(q => typeof q === "string"),
    "Alle queries moeten strings zijn"
);

// lookupTable is nu een SQL UDF, niet meer JS, en verpakt in een test-and-swap script.
const lookup_op = result.find(q => typeof q === "string" && q.includes("lookupTable"));
assert.ok(lookup_op, "Verwacht een operation voor lookupTable");

assert.ok(
    !lookup_op.includes("LANGUAGE js"),
    "lookupTable mag geen JavaScript UDF meer zijn"
);
assert.ok(
    lookup_op.includes("RETURNS STRING") && lookup_op.includes(" AS ( "),
    "lookupTable moet een SQL UDF zijn (RETURNS STRING AS ( ... ))"
);
assert.ok(
    lookup_op.includes("REGEXP_EXTRACT") &&
    lookup_op.includes("STRING_AGG") &&
    lookup_op.includes("JSON_VALUE_ARRAY") &&
    lookup_op.includes("NORMALIZE_AND_CASEFOLD"),
    "lookupTable SQL body mist verwachte BigQuery-functies"
);
assert.ok(
    lookup_op.includes("ORDER BY CHAR_LENGTH(opt) DESC"),
    "lookupTable moet langste-merk-eerst prioriteit hanteren"
);

// Test-and-swap structuur: BEGIN ... END met _test create vóór ASSERTs vóór echte create vóór DROP.
assert.ok(lookup_op.trim().startsWith("BEGIN"), "lookupTable operation moet BEGIN ... END zijn");
assert.ok(lookup_op.trim().endsWith("END;"), "lookupTable operation moet eindigen op END;");

const test_create_idx = lookup_op.indexOf("`test_db.rawdata.lookupTable_test`");
const first_assert_idx = lookup_op.indexOf("ASSERT (");
const real_create_idx = lookup_op.indexOf("`test_db.rawdata.lookupTable`");
const drop_idx = lookup_op.indexOf("DROP FUNCTION IF EXISTS `test_db.rawdata.lookupTable_test`");

assert.ok(test_create_idx !== -1, "lookupTable_test moet aangemaakt worden");
assert.ok(first_assert_idx !== -1, "Er moeten ASSERT-statements zijn");
assert.ok(real_create_idx !== -1, "Echte lookupTable moet aangemaakt worden");
assert.ok(drop_idx !== -1, "lookupTable_test moet gedropt worden");

assert.ok(
    test_create_idx < first_assert_idx,
    "lookupTable_test moet aangemaakt worden vóór de ASSERTs"
);
assert.ok(
    first_assert_idx < real_create_idx,
    "ASSERTs moeten draaien vóór de echte lookupTable wordt vervangen"
);
assert.ok(
    real_create_idx < drop_idx,
    "Echte lookupTable moet aangemaakt worden vóór de _test wordt gedropt"
);

// Voor elke testcase moet er een ASSERT zijn met de verwachte uitkomst.
for (const tc of test_cases) {
    const fragment = tc.expected.trim().toUpperCase() === "NULL"
        ? `${tc.needle}, '` // ASSERT-call moet de needle bevatten
        : tc.expected;
    assert.ok(
        lookup_op.includes(fragment),
        `Verwachte ASSERT-fragment voor "${tc.description}" ontbreekt in lookupTable operation`
    );
}

const assert_count = (lookup_op.match(/ASSERT \(/g) || []).length;
assert.strictEqual(
    assert_count,
    test_cases.length,
    `Verwacht ${test_cases.length} ASSERTs, kreeg er ${assert_count}`
);

console.log("✓ implementation tests passed");
