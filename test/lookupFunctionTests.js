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