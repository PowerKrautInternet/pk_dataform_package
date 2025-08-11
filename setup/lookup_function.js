let lookup_function_string = `
function removeAccents(strAccents) {
    strAccents = strAccents ?? "";
    strAccents = strAccents.split('');
    let strAccentsOut = [];
    const strAccentsLen = strAccents.length;
    const accents = "ÀÁÂÃÄÅàáâãäåÒÓÔÕÕÖØòóôõöøÈÉÊËèéêëðÇçÐÌÍÎÏìíîïÙÚÛÜùúûüÑñŠšŸÿýŽž";
    const accentsOut = "AAAAAAaaaaaaOOOOOOOooooooEEEEeeeeeCcDIIIIiiiiUUUUuuuuNnSsYyyZz";
    for (let y = 0; y < strAccentsLen; y++) {
        if (accents.indexOf(strAccents[y]) !== -1) {
            strAccentsOut[y] = accentsOut.substring(accents.indexOf(strAccents[y])-1, accents.indexOf(strAccents[y]));
        } else strAccentsOut[y] = strAccents[y];
    }
    return strAccentsOut.join('');
}

function lookup(needle, haystack){
    const lookupTable = JSON.parse(haystack);
    let options = []
    for (let m of lookupTable) {
        m = removeAccents(m);
        m = m.replace(/[^a-zA-Z0-9]/gi, " ");
        m = m.replace(/\\s+/gi, " ");
        m = m.replace(/\\\\/gi, " ");
        m = m.toLowerCase();
        options.push(m);
    }
    string = removeAccents(needle);
    string = string.replace(/[^a-zA-Z0-9]/gi, " ");
    string = string.replace(/\\s+/gi, " ");
    string = string.replace(/\\\\/gi, " ");
    string = string.toLowerCase();
    string = string.split(" ")
    
    for (const word of string) {
        for (const option of options) {
            if(option === word){
                return (option.charAt(0).toUpperCase() + option.slice(1));
            }
        }
    }
}
`;

eval(lookup_function_string);

lookup_function_string += '\nreturn lookup(needle, haystack);';

module.exports = {lookup_function_string, lookup}