let lasttransaction = require("./lasttransaction");
let pk = require("../sources");

function getLookup(){
    pk.addSource({"config":{"database": dataform.projectConfig.defaultDatabase, "schema": "rawdata"}, "name": "lookupTable", "type": "function"})
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
            query.push(lasttransaction(sources[s]));
        }
    }
    return query
}

module.exports = {setupFunctions};