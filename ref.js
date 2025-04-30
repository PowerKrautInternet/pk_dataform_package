const pk = require("./index");
let refs = []

function ref(p1, p2) {
    let sources = pk.getSources();
    let ref = ""
    for(let s in sources) {
        if(sources[s].name == p1 || (typeof p2 != "undefined" && sources[s].name == p2 && sources[s].schema == p1)){
            ref = "`" + sources[s].database + "." + sources[s].schema
            if(sources[s].schema != "rawdata" && sources[s].schema != "googleSheets" && dataform.projectConfig.schemaSuffix != "") { ref += "_" + dataform.projectConfig.schemaSuffix }
            ref += "." + sources[s].name + "` "
            refs.push({
                "name": sources[s].name,
                "schema": sources[s].schema,
                "database": sources[s].database
            })
            return ref
        }
    }
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
    return refs
}

module.exports = {ref, getRefs};