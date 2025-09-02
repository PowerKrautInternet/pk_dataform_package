let string = `


function removeAccents(strAccents) {
    if (!strAccents) return "";  // handles null, undefined, empty string
    strAccents = strAccents.split('');
    const accents =    "ÀÁÂÃÄÅàáâãäåÒÓÔÕÕÖØòóôõöøÈÉÊËèéêëðÇçÐÌÍÎÏìíîïÙÚÛÜùúûüÑñŠšŸÿýŽž";
    const accentsOut = "AAAAAAaaaaaaOOOOOOOooooooEEEEeeeeeCcDIIIIiiiiUUUUuuuuNnssYyyZz";
    for (let i in accents) {
        let j = strAccents.indexOf(accents[i]);
        if (j !== -1) {
            strAccents[j] = accentsOut[i];
        }
    }
    return strAccents.join('');
}

function lookupTable(needle, haystack) {
    if (!needle || !haystack) return null;  // prevent null input crash

    const lookupTable = JSON.parse(haystack);
    let options = [];

    for (let m of lookupTable) {
        m = removeAccents(m);
        m = m.replace(/[^a-zA-Z0-9]/gi, " ")
             .replace(/\\s+/gi, " ")
             .replace(/\\\\/gi, " ")
             .toLowerCase();
        options.push(m);
    }

    let string = removeAccents(needle);
    string = string.replace(/[^a-zA-Z0-9]/gi, " ")
                   .replace(/\\s+/gi, " ")
                   .replace(/\\\\/gi, " ")
                   .toLowerCase();

    if (options.length === 0) return null;

    const pattern = '\\\\b(' + options.sort((a, b) => b.length - a.length).join("|") + ')\\\\b';
    const regexp = new RegExp(pattern, "gim");

    let matches = [...string.matchAll(regexp)];
    if (matches.length < 1) {
        return null; // return null if empty
    } else if (matches[0][0].length > 3) {
        return matches[0][0].charAt(0).toUpperCase() + matches[0][0].slice(1);
    } else {
        return matches[0][0].toUpperCase();
    }
}

return lookupTable(needle, haystack);

`;

module.exports = string
