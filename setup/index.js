let pk = require("../sources");
let lasttransaction = require("../setup/lasttransaction");

const {addSource} = require("../sources");
let refs = []

function ref(p1, p2) {
    let sources = pk.getSources();
    let ref = []
    let NrFound = 0;
    for(let s in sources) {
        if( //if the ref has only one parameter it has to be the name, when there are 2 parameter the second wil be the name. (name is interchangable with alias)
            (typeof p2 == "undefined" && (sources[s].alias === p1 || (sources[s].name === p1 && typeof sources[s].alias == 'undefined') ) )
            ||
            (typeof p2 != "undefined" && (sources[s].alias === p2 || (sources[s].name === p2 && typeof sources[s].alias == 'undefined') ) && sources[s].schema === p1)
        ){
            ref[NrFound] = "`" + sources[s].database + "." + sources[s].schema
            //voeg een suffix voor development toe. Alleen toevoegen als het niet om brondata gaat (gedefineerd als rawdata of googleSheets)
            if(sources[s].schema !== "rawdata" && sources[s].schema !== "googleSheets" && dataform.projectConfig.schemaSuffix !== "") { ref[NrFound] += "_" + dataform.projectConfig.schemaSuffix }
            ref[NrFound] += "." + sources[s].name + "` "
            if(sources[s].type !== "function") {
                refs.push({
                    "name": sources[s].name,
                    "schema": sources[s].schema,
                    "database": sources[s].database
                })
            } else {
                return "`" + sources[s].database + "`.rawdata." + p1
            }
            NrFound++;
        }
    }


    //if a ref was found than return al the refs that where found in an query that will be implemented in a 'from'
    if(NrFound > 0) {
        let refQuery = ""
        refQuery = "\n(SELECT * FROM \n";
        for (let r in ref) {
            if (r > 0) {
                refQuery += "UNION ALL "
            }
            refQuery += ref[r];
        }
        refQuery +=" \n)"
        return refQuery;
    }

    //If none is found the following will try and give an estimated source with default values
    ref += "`" + dataform.projectConfig.defaultDatabase + "."
    if(typeof p2 == "undefined") {
        ref += dataform.projectConfig.defaultSchema + "." + p1
        refs.push({
            "name": p1,
            "schema": dataform.projectConfig.defaultSchema,
            "database": dataform.projectConfig.defaultDatabase
        })
    } else {
        ref += p1 + "." + p2
        refs.push({
            "name": p2,
            "schema": p1,
            "database": dataform.projectConfig.defaultDatabase
        })
    }
    ref += "` "
    return ref;
}

function getRefs(){
    let dependencies = refs;
    refs = []
    return dependencies
}

function getLookup(){
    addSource({"config":{"database": dataform.projectConfig.defaultDatabase, "schema": "rawdata"}, "name": "lookupTable", "type": "function"})
    return `
        CREATE OR REPLACE FUNCTION ${"`" + dataform.projectConfig.defaultDatabase + ".rawdata.lookupTable`"} (arg_needle STRING, arg_table_as_json STRING) RETURNS STRING LANGUAGE js AS R"""
        function lookupTable(needle, haystack) {
            const lookupTable = JSON.parse(haystack);
    
    
            for (var item in lookupTable) {
                let normalizedValue = removeAccents(lookupTable[item]);
                let normalizedNeedle = removeAccents(needle);
                normalizedNeedle = normalizedNeedle.replace(/[^a-zA-Z0-9]/gi, " ");
                normalizedValue = normalizedValue.replace(/[^a-zA-Z0-9]/gi, " ");
                normalizedNeedle = normalizedNeedle.replace(/\s+/gi, " ");
                normalizedValue = normalizedValue.replace(/\s+/gi, " ");
    
                if (normalizedNeedle.match(new RegExp('.*\\b' + normalizedValue + '\\b.*', 'gi'))) {
                    return lookupTable[item];
                }
            }
    
            return null;
        }
    
        function removeAccents(strAccents) {
            var strAccents = strAccents.split('');
            var strAccentsOut = new Array();
            var strAccentsLen = strAccents.length;
            var accents =    "ÀÁÂÃÄÅàáâãäåÒÓÔÕÕÖØòóôõöøÈÉÊËèéêëðÇçÐÌÍÎÏìíîïÙÚÛÜùúûüÑñŠšŸÿýŽž";
            var accentsOut = "AAAAAAaaaaaaOOOOOOOooooooEEEEeeeeeCcDIIIIiiiiUUUUuuuuNnSsYyyZz";
            for (var y = 0; y < strAccentsLen; y++) {
                if (accents.indexOf(strAccents[y]) != -1) {
                    strAccentsOut[y] = accentsOut.substr(accents.indexOf(strAccents[y]), 1);
                } else
                    strAccentsOut[y] = strAccents[y];
            }
            strAccentsOut = strAccentsOut.join('');
    
            return strAccentsOut;
        }
    
        return lookupTable(arg_needle, arg_table_as_json);
        """;
    `
}

function setupFunctions(sources){
    let query = []
    query[0] = getLookup();
    for(let s in sources){
        if(sources[s].name.endsWith("DataProducer")){
            query[s+1] = lasttransaction(sources[s].name);
        }
    }
    return query
}

module.exports = {ref, setupFunctions, getRefs};