let string = `


function removeAccents(strAccents = "") {
    strAccents = strAccents.split('');
    const accents =    "ÀÁÂÃÄÅàáâãäåÒÓÔÕÕÖØòóôõöøÈÉÊËèéêëðÇçÐÌÍÎÏìíîïÙÚÛÜùúûüÑñŠšŸÿýŽž";
    const accentsOut = "AAAAAAaaaaaaOOOOOOOooooooEEEEeeeeeCcDIIIIiiiiUUUUuuuuNnssYyyZz";
    for (let i in accents){
        let j = strAccents.indexOf(accents[i]);
        if(j !== -1) {
            strAccents[j] = accentsOut[i];
        }
    }
    return strAccents.join('');
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
    
    const pattern = '\\\\b('+options.sort((a, b) => b.length - a.length).join("|")+')\\\\b'
    const regexp = new RegExp(pattern, "gim");
    
    let matches = [...string.matchAll(regexp)];
    if(matches.length < 1){
        return null  //return null if empty
    } else if(matches[0][0].length > 3){
        return matches[0][0].charAt(0).toUpperCase() + matches[0][0].slice(1)   
    } else {
        return matches[0][0].toUpperCase()
    }
}

`;

module.exports = string