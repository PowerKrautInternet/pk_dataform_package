let lasttransaction = require("./lasttransaction");
let pk = require("../sources");

function getLookup(){
    pk.addSource({"config":{"database": dataform.projectConfig.defaultDatabase, "schema": "rawdata"}, "name": "lookupTable", "type": "function"})
    return `
        CREATE OR REPLACE FUNCTION ${"`" + dataform.projectConfig.defaultDatabase + ".rawdata.lookupTable`"} (needle STRING, haystack STRING) RETURNS STRING LANGUAGE js AS R"""
        const lookupTable = JSON.parse(haystack);


    for (const item in lookupTable) {
        let normalizedValue = removeAccents(lookupTable[item]);
        let normalizedNeedle = removeAccents(needle);
        normalizedNeedle = normalizedNeedle.replace(/[^a-zA-Z0-9]/gi, " ");
        normalizedValue = normalizedValue.replace(/[^a-zA-Z0-9]/gi, " ");
        normalizedNeedle = normalizedNeedle.replace(/\s+/gi, " ");
        normalizedValue = normalizedValue.replace(/\s+/gi, " ");

        if (normalizedNeedle.match(new RegExp('.*'+normalizedValue+'.*', 'gi'))) {
            return lookupTable[item];
        }
    }

    return null;
function removeAccents(strAccents) {
    strAccents = strAccents ?? "";
    strAccents = strAccents.split('');
    let strAccentsOut = [];
    const strAccentsLen = strAccents.length;
    const accents = "ÀÁÂÃÄÅàáâãäåÒÓÔÕÕÖØòóôõöøÈÉÊËèéêëðÇçÐÌÍÎÏìíîïÙÚÛÜùúûüÑñŠšŸÿýŽž";
    const accentsOut = "AAAAAAaaaaaaOOOOOOOooooooEEEEeeeeeCcDIIIIiiiiUUUUuuuuNnSsYyyZz";
    for (let y = 0; y < strAccentsLen; y++) {
        if (accents.indexOf(strAccents[y]) !== -1) {
            strAccentsOut[y] = accentsOut.substring(accents.indexOf(strAccents[y]), 1);
        } else
            strAccentsOut[y] = strAccents[y];
    }
    return strAccentsOut.join('');
}
        """;
    `
}

function getAdres(){
    pk.addSource({"config":{"database": dataform.projectConfig.defaultDatabase, "schema": "rawdata"}, "name": "getAdres", "type": "function"})
    return `
        CREATE OR REPLACE FUNCTION ${"`" + dataform.projectConfig.defaultDatabase + ".rawdata.getAdres`"} (json_row STRING, field_name STRING) RETURNS STRING LANGUAGE js AS R"""
        function getAdres(obj, field_name) {
 let response = JSON.parse(obj);
 return ((response && response.slice().length > 0) ? response.slice().pop()[field_name] : null)
}
return getAdres(json_row, field_name);""";
    `
}

function getEmail(){
    pk.addSource({"config":{"database": dataform.projectConfig.defaultDatabase, "schema": "rawdata"}, "name": "getEmail", "type": "function"})
    return `
        CREATE OR REPLACE FUNCTION ${"`" + dataform.projectConfig.defaultDatabase + ".rawdata.getEmail`"} (json_row STRING) RETURNS STRING LANGUAGE js AS R"""
        function getEmail(obj) {
 let contact = JSON.parse(obj).slice().filter((item, index) => { return item.contactwijzeType == "E-mail" }).pop();
 return ((contact) ? contact.value : null)
}
return getEmail(json_row);""";
    `
}

function getTelefoon(){
    pk.addSource({"config":{"database": dataform.projectConfig.defaultDatabase, "schema": "rawdata"}, "name": "getTelefoon", "type": "function"})
    return `
        CREATE OR REPLACE FUNCTION ${"`" + dataform.projectConfig.defaultDatabase + ".rawdata.getTelefoon`"} (json_row STRING, field_name STRING) RETURNS STRING LANGUAGE js AS R"""
        function getTelefoon(obj) {
 let contact = JSON.parse(obj).slice().filter((item, index) => { return item.contactwijzeType == "Telefoon" }).pop();
 return ((contact) ? contact.value : null)
}
return getTelefoon(json_row);""";
    `
}

function setupFunctions(sources){
    let query = []
    query[0] = getLookup();
    query[1] = getAdres();
    query[2] = getEmail();
    query[3] = getTelefoon();
    for(let s in sources){
        if(typeof sources[s].name != "undefined" && sources[s].name.endsWith("DataProducer")){
            query.push(lasttransaction(sources[s]));
        }
    }
    return query
}

module.exports = {setupFunctions};