const pk = require("../sources");
let refs = []

function ref(p1, p2) {
    let sources = pk.getSources();
    let ref = []
    let NrFound = 0;
    for(let s in sources) {
        if( //if the ref has only one parameter it has to be the name, when there are 2 parameter the second wil be the name.
            (typeof p2 == "undefined" &&sources[s].name == p1)
            ||
            (typeof p2 != "undefined" && sources[s].name == p2 && sources[s].schema == p1)
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

module.exports = {ref, getRefs};