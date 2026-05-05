// test/lookupFunctionTests.js
//framework ?

global.dataform = {
    projectConfig: {
        defaultDatabase: process.env.DATAFORM_DEFAULT_DB || "test_db"
    }
};

const assert = require("assert");
const { setupFunctions, function_config } = require("../setup");
const { test_cases } = require("../setup/lookup_table_sql_tests");

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

// 1. Oude lookupTable blijft een JavaScript UDF (externe consumers leunen erop).
const legacy_ddl = result.find(q =>
    typeof q === "string" &&
    q.includes("`test_db.rawdata.lookupTable`") &&
    q.includes("LANGUAGE js")
);
assert.ok(legacy_ddl, "Oude JavaScript lookupTable moet behouden blijven");
assert.ok(
    legacy_ddl.includes("function lookupTable("),
    "Oude lookupTable moet de originele JS body bevatten"
);

// 2. Nieuwe lookup_table_sql is een SQL UDF, in een test-and-swap script.
const sql_op = result.find(q => typeof q === "string" && q.includes("lookup_table_sql"));
assert.ok(sql_op, "Verwacht een operation voor lookup_table_sql");
assert.ok(!sql_op.includes("LANGUAGE js"), "lookup_table_sql mag geen JavaScript UDF zijn");
assert.ok(
    sql_op.includes("RETURNS STRING") && sql_op.includes(" AS ( "),
    "lookup_table_sql moet een SQL UDF zijn (RETURNS STRING AS ( ... ))"
);
assert.ok(
    sql_op.includes("REGEXP_EXTRACT") &&
    sql_op.includes("STRING_AGG") &&
    sql_op.includes("JSON_VALUE_ARRAY") &&
    sql_op.includes("NORMALIZE_AND_CASEFOLD"),
    "lookup_table_sql SQL body mist verwachte BigQuery-functies"
);
assert.ok(
    sql_op.includes("ORDER BY CHAR_LENGTH(opt) DESC"),
    "lookup_table_sql moet langste-merk-eerst prioriteit hanteren"
);

// 3. Test-and-swap structuur voor lookup_table_sql.
assert.ok(sql_op.trim().startsWith("BEGIN"), "lookup_table_sql operation moet BEGIN ... END zijn");
assert.ok(sql_op.trim().endsWith("END;"), "lookup_table_sql operation moet eindigen op END;");

const test_create_idx = sql_op.indexOf("`test_db.rawdata.lookup_table_sql_test`");
const first_assert_idx = sql_op.indexOf("ASSERT (");
const real_create_idx = sql_op.indexOf("`test_db.rawdata.lookup_table_sql`");
const drop_idx = sql_op.indexOf("DROP FUNCTION IF EXISTS `test_db.rawdata.lookup_table_sql_test`");

assert.ok(test_create_idx !== -1, "lookup_table_sql_test moet aangemaakt worden");
assert.ok(first_assert_idx !== -1, "Er moeten ASSERT-statements zijn");
assert.ok(real_create_idx !== -1, "Echte lookup_table_sql moet aangemaakt worden");
assert.ok(drop_idx !== -1, "lookup_table_sql_test moet gedropt worden");

assert.ok(
    test_create_idx < first_assert_idx,
    "lookup_table_sql_test moet aangemaakt worden vóór de ASSERTs"
);
assert.ok(
    first_assert_idx < real_create_idx,
    "ASSERTs moeten draaien vóór de echte lookup_table_sql wordt (her)gemaakt"
);
assert.ok(
    real_create_idx < drop_idx,
    "Echte lookup_table_sql moet aangemaakt worden vóór de _test wordt gedropt"
);

// Voor elke testcase moet er een ASSERT zijn met de verwachte uitkomst.
for (const tc of test_cases) {
    const fragment = tc.expected.trim().toUpperCase() === "NULL"
        ? `${tc.needle}, '`
        : tc.expected;
    assert.ok(
        sql_op.includes(fragment),
        `Verwachte ASSERT-fragment voor "${tc.description}" ontbreekt in lookup_table_sql operation`
    );
}

const assert_count = (sql_op.match(/ASSERT \(/g) || []).length;
assert.strictEqual(
    assert_count,
    test_cases.length,
    `Verwacht ${test_cases.length} ASSERTs, kreeg er ${assert_count}`
);

console.log("✓ implementation tests passed");
