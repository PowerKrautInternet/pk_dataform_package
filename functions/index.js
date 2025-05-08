const pk = require("../sources");
let refs = []

function ref(p1, p2) {
    let sources = pk.getSources();
    let ref = []
    let NrFound = 0;
    for(let s in sources) {
        if( //if the ref has only one parameter it has to be the name, when there are 2 parameter the second wil be the name. (name is interchangable with alias)
            (typeof p2 == "undefined" && (sources[s].name == p1 || sources[s].type == p1) )
            ||
            (typeof p2 != "undefined" && (sources[s].name == p2 || sources[s].type == p2) && sources[s].schema == p1)
        ){
            ref[NrFound] = "`" + sources[s].database + "." + sources[s].schema
            //voeg een suffix voor development toe. Alleen toevoegen als het niet om brondata gaat (gedefineerd als rawdata of googleSheets)
            if(sources[s].schema != "rawdata" && sources[s].schema != "googleSheets" && dataform.projectConfig.schemaSuffix != "") { ref[NrFound] += "_" + dataform.projectConfig.schemaSuffix }
            ref[NrFound] += "." + sources[s].name + "` "
            refs.push({
                "name": sources[s].name,
                "schema": sources[s].schema,
                "database": sources[s].database
            })
            NrFound++;
        }
    }

    //if a ref was found than return al the refs that where found in an query that will be implemented in an 'from'
    if(NrFound > 0) {
        let refQuery = "";
        for (let r in ref) {
            if (r > 0) {
                refQuery += "UNION ALL "
            }
            refQuery = ref[r];
        }
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
    let bufferRefs = refs;
    refs = []
    return bufferRefs
}

function getLookup(){
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

module.exports = {ref, getRefs, getLookup};