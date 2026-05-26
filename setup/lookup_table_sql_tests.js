let FunctionObject = require("./function_helper");

const merken_haystack = `'["Abarth","AC","volkswagen","audi","alfa romeo","skoda","citroen","vw","vw professional"]'`;

const test_cases = [
    {
        needle: `'this is an test for audi, and not for volkswagen'`,
        expected: `'Audi'`,
        description: "match Audi tussen 2 merken"
    },
    {
        needle: `"This is another test with some $ extra ' special \\n characters to see if it fails, but with volkswagen and not audi"`,
        expected: `'Volkswagen'`,
        description: "speciale karakters breken matching niet"
    },
    {
        needle: `'Will i buy an alfa romeo or an audi? Nobody knows!'`,
        expected: `'Alfa romeo'`,
        description: "merknaam met spatie wordt herkend"
    },
    {
        needle: `'Now i want to maybe buy an Škoda or maybe an Citroën'`,
        expected: `'Skoda'`,
        description: "diakrieten worden genormaliseerd"
    },
    {
        needle: `'Now i want an beautiful vw professional truck'`,
        expected: `'Vw professional'`,
        description: "langste merk wint van kortere overlap (vw professional > vw)"
    },
    {
        needle: `'Now i want an beautiful vw truck'`,
        expected: `'VW'`,
        description: "merk van 2 chars wordt volledig uppercase"
    },
    {
        needle: `NULL`,
        expected: `NULL`,
        description: "NULL needle blijft NULL"
    }
];

function buildAssert(fnName, call, expected, description, idx) {
    const condition = expected.trim().toUpperCase() === "NULL"
        ? `${call} IS NULL`
        : `${call} = ${expected}`;
    const message = `${fnName} test ${idx + 1} (${description}) failed`.replace(/'/g, "''");
    const stmt = `ASSERT (${condition}) AS '${message}'`.replace(/'/g, "''");
    return `EXECUTE IMMEDIATE '${stmt}';`;
}

function lookupTableSqlTestAndSwap(functionObject) {
    const test_function = new FunctionObject({
        database: functionObject.database,
        schema: functionObject.schema,
        name: `${functionObject.name}_test`,
        vars: functionObject.varsForFunction,
        function: functionObject.sql_object.function,
        function_type: functionObject.type
    });

    const test_name = `\`${test_function.database}.${test_function.schema}.${test_function.name}\``;

    const asserts = test_cases
        .map((tc, i) => buildAssert(functionObject.name, `${test_name}(${tc.needle}, ${merken_haystack})`, tc.expected, tc.description, i))
        .join("\n  ");

    return `BEGIN
  -- Stap 1: nieuwe SQL UDF onder test-naam, oude ${functionObject.name} blijft (indien aanwezig) intact
  ${test_function.sql}

  -- Stap 2: gedragstesten draaien tegen de test-versie
  ${asserts}

  -- Stap 3: alle asserts geslaagd, (her)maak nu de echte ${functionObject.name}
  ${functionObject.sql}

  -- Stap 4: opruimen
  DROP FUNCTION IF EXISTS ${test_name};
END;`;
}

module.exports = { lookupTableSqlTestAndSwap, test_cases };
