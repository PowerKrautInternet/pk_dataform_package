let lasttransaction = require("./lasttransaction");
let googleSheetTable = require("./googleSheetTable");
let FunctionObject = require("./function_helper");
let pk = require("../sources");
const {ifSource, addSource} = require("../sources");

let function_config = [
    {
        database: dataform.projectConfig.defaultDatabase,
        schema: "rawdata",
        name: "lookupTable",
        vars: {needle: "STRING", haystack: "STRING"},
        function: require("./lookup_function")
    },
    {
        database: dataform.projectConfig.defaultDatabase,
        schema: "rawdata",
        name: "getEmail",
        vars: {json_row: "STRING"},
        function: require("./getEmail_function")
    },
    {
        database: dataform.projectConfig.defaultDatabase,
        schema: "rawdata",
        name: "getAdres",
        vars: {json_row: "STRING", field_name: "STRING"},
        function: require("./getAdres_function")
    },
    {
        database: dataform.projectConfig.defaultDatabase,
        schema: "rawdata",
        name: "getTelefoon",
        vars: {json_row: "STRING"},
        function: require("./getTelefoon_function")
    },
]

let function_array = function_config.map(config => new FunctionObject(config));

function setupFunctions(sources){
    let query = []
    let declared = {}
    for(let s in sources){
        if(typeof sources[s].name != "undefined" && sources[s].name.endsWith("DataProducer") && declared[sources[s].name] !== true){
            query.push(lasttransaction(sources[s]));
            declared[sources[s].name] = true;
        } else if (typeof sources[s].schema != "undefined" && typeof sources[s].name != "undefined" && sources[s].schema === "googleSheets" && declared[sources[s].alias ?? sources[s].name] !== true) {
            query.push(googleSheetTable(sources[s]));
            declared[sources[s].alias ?? sources[s].name] = true;
        }
    }

    //Add functions to the dataform operation query
    for(let f of function_array){
        query.push(f.sql);   //add to query
        addSource(f.source); //make the function available for internal use
    }

    return query
}

module.exports = {setupFunctions};
