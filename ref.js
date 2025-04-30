const pk = require("./index");

function ref(p1, p2) {
    let sources = pk.getSources();
    let ref = ""
    for(let s in sources) {
        if(sources[s].name == p1 || (typeof p2 != "undefined" && sources[s].name == p2 && sources[s].schema == p1)){
            ref = "`" + sources[s].database + "." + sources[s].schema
            if(sources[s].schema != "rawdata" && sources[s].schema != "googleSheets" && dataform.projectConfig.schemaSuffix != "") { ref += "_" + dataform.projectConfig.schemaSuffix }
            ref += "." + sources[s].name + "` "
            return ref
        }
    }
    ref += "`" + dataform.projectConfig.defaultDatabase + "."
    if(typeof p2 == "undefined") {
        ref += dataform.projectConfig.defaultSchema + "." + p1
    } else {
        ref += p1 + "." + p2
    }
    ref += "` "
    return ref;
}

module.exports = ref;