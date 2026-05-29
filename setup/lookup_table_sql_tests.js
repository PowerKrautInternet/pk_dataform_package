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

function inlineBody(body, needleExpr, haystackExpr) {
    return body
        .replace(/\bneedle\b/g, needleExpr)
        .replace(/\bhaystack\b/g, haystackExpr);
}

function buildAssert(fnName, inlinedBody, expected, description, idx) {
    const condition = expected.trim().toUpperCase() === "NULL"
        ? `${inlinedBody} IS NULL`
        : `${inlinedBody} = ${expected}`;
    const message = `${fnName} test ${idx + 1} (${description}) failed`.replace(/'/g, "\\'");
    return `ASSERT (${condition}) AS '${message}';`;
}

function lookupTableSqlTestAndSwap(functionObject) {
    const body = functionObject.sql_object.function;

    const asserts = test_cases
        .map((tc, i) => buildAssert(functionObject.name, inlineBody(body, tc.needle, merken_haystack), tc.expected, tc.description, i))
        .join("\n  ");

    return `BEGIN
  -- Stap 1: inline ASSERTs valideren de SQL body zonder een test-UDF aan te maken.
  -- Een persistent UDF die in dit script wordt aangemaakt is niet altijd zichtbaar
  -- voor opvolgende statements in hetzelfde BigQuery script-job (catalog-cache quirk),
  -- daarom testen we de body direct als scalar subquery.
  ${asserts}

  -- Stap 2: alle asserts geslaagd, (her)maak nu de echte ${functionObject.name}
  ${functionObject.sql}
END;`;
}

module.exports = { lookupTableSqlTestAndSwap, test_cases };
