// test/lookupFunctionTests.js

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

for(let config of function_config){
    eval(require("../setup/lookup_function"))
}

const haystack = "[\"Abarth\",\"AC\", \"volkswagen\", \"audi\", \"alfa romeo\", \"skoda\", \"citroen\", \"vw\", \"vw professional\"]"

let test1 = lookupTable("this is an test for audi, and not for volkswagen", haystack)
console.log(`Test 1: ${test1}`)
assert.ok(
     test1 === "Audi",
    "Test 1: 2 merken"
)

let test2 = lookupTable("This is another test with some $ extra ' special \n characters to see if it fails, but with volkswagen and not audi", haystack)
console.log(`Test 2: ${test2}`)
assert.ok(
    test2 === "Volkswagen",
    "Test 2: 2 merken with breaking characters"
)

let test3 = lookupTable("Will i buy an alfa romeo or an audi? Nobody knows!", haystack)
console.log(`Test 3: ${test3}`)
assert.ok(
    test3 === "Alfa romeo",
    "Test 3: 2 merken with space characters"
)

function test_an_function(name, function_result, result, description) {
    console.log(name + ": " + function_result)
    assert.ok(
        result === function_result,
        name + ": " + description
    )
}

test_an_function('test 4', lookupTable("Now i want to maybe buy an Škoda or maybe an Citroën", haystack), 'Skoda', '2 merken met speciale characters')
test_an_function('test 5', lookupTable("Now i want an beautiful vw professional truck", haystack), 'Vw professional', 'VW professional ipv VW')
test_an_function('test 6', lookupTable("Now i want an beautiful vw truck", haystack), 'VW', 'VW full word uppercase')
test_an_function('Test 7; NULL value test', lookupTable(null, haystack), null, "Null stays null")


console.log("✓ implementation tests passed");
//
// /**
//   @brief Integration test for verifying a DuckDB SQL macro (email_cleaner).
//  */
//
// let email_cleaner = null;
// for(let f of function_config){
//     if(f.name === "email_cleaner"){
//         email_cleaner = f.function;
//     }
// }
//
// if(email_cleaner == null){
//     throw new Error(`Test error! test/email_cleaner`);
// }
//
// const duckdb = require("duckdb");
//
// (async () => {
//     const db = new duckdb.Database(":memory:");
//     const conn = db.connect();
//
//     const run = (sql) =>
//         new Promise((resolve, reject) => conn.run(sql, (err) => (err ? reject(err) : resolve())));
//
//     const all = (sql) =>
//         new Promise((resolve, reject) => conn.all(sql, (err, res) => (err ? reject(err) : resolve(res))));
//
//     // ✅ DuckDB ondersteunt CREATE MACRO in plaats van CREATE FUNCTION
//     await run(`
//     CREATE OR REPLACE MACRO email_cleaner(email) AS
//       (
//     -- === BYPASS: skip de hele functie als er een spatie is zonder e-mailadres tussen <> ===
//     CASE
//       WHEN REGEXP_CONTAINS(raw_input, r'\s')
//            AND NOT REGEXP_CONTAINS(raw_input, r'<[^>]+@[^\s>]+\.[A-Za-z]{2,}>')
//         THEN raw_input
//       ELSE (
//         WITH base AS (
//           SELECT COALESCE(
//             REGEXP_EXTRACT(raw_input, r'<([^>]+)>'),
//             raw_input
//           ) AS email_input
//         ),
//
//         emails AS (
//           SELECT
//             TRANSLATE(
//               REGEXP_REPLACE(REGEXP_REPLACE(email_input, r'[#!$^&*\(\)\[\]\{\}\:\;\"\'\<\,\>\?\/]', ""), r'([.\-%+_@]){2,}', r'\\1'),
//               'ÁÀÂÄáàâäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÖóòôöÚÙÛÜúùûüÑñÇç',
//               'AAAAaaaaEEEEeeeeIIIIiiiiOOOOooooUUUUuuuuNnCc'
//             ) AS email
//           FROM base
//         ),
//
//         split_email AS (
//           SELECT
//             REGEXP_EXTRACT(email, r'([a-zA-Z0-9._%+-]+)@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}') AS first,
//             REGEXP_EXTRACT(email, r'[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+)\\.[a-zA-Z]{2,}') AS middle,
//             REGEXP_EXTRACT(email, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.([a-zA-Z]{2,})') AS last,
//             email
//           FROM emails
//         ),
//
//         trimmed AS (
//           SELECT
//             REGEXP_REPLACE(LOWER(first), r'^[._%+-]+|[._%+-]+$', '') AS first,
//             REGEXP_REPLACE(LOWER(middle), r'^[.-]+|[.-]+$', '') AS middle,
//             CASE
//               WHEN REGEXP_CONTAINS(LOWER(last), 'com') THEN 'com'
//               WHEN REGEXP_CONTAINS(LOWER(last), 'nl') THEN 'nl'
//               WHEN REGEXP_CONTAINS(LOWER(last), 'c') AND REGEXP_CONTAINS(LOWER(last), 'o') AND REGEXP_CONTAINS(LOWER(last), 'm') THEN 'com'
//               WHEN REGEXP_CONTAINS(LOWER(last), 'n') AND REGEXP_CONTAINS(LOWER(last), 'l') THEN 'nl'
//               WHEN REGEXP_CONTAINS(LOWER(last), 'c') OR REGEXP_CONTAINS(LOWER(last), 'o') OR REGEXP_CONTAINS(LOWER(last), 'm') THEN 'com'
//               WHEN REGEXP_CONTAINS(LOWER(last), 'n') OR REGEXP_CONTAINS(LOWER(last), 'l') THEN 'nl'
//               ELSE LOWER(last)
//             END AS last,
//             email
//           FROM split_email
//         )
//
//         SELECT
//           CASE
//             WHEN first IS NULL OR middle IS NULL OR last IS NULL THEN email
//             ELSE CONCAT(first, '@', middle, '.', last)
//           END
//         FROM trimmed
//       )
//     END
//   )
//   `);
//
//     // Test 1: normale invoer
//     const rows1 = await all(`SELECT email_cleaner('  TEST@Example.COM  ') AS cleaned;`);
//     console.log("Test 1:", rows1[0]);
//     assert.strictEqual(rows1[0].cleaned, "test@example.com");
//
//     // Test 2: NULL-waarde
//     const rows2 = await all(`SELECT email_cleaner(NULL) AS cleaned;`);
//     console.log("Test 2:", rows2[0]);
//     assert.strictEqual(rows2[0].cleaned, null);
//
//     console.log("✓ SQL tests passed");
//     conn.close();
// })();