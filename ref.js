const pk = require("./index");

function ref(name) {
    let sources = pk.getSources();
    for(let s in sources) {
        if(sources[s].name == name){
            let ref = "`" + sources[s].database + "." + sources[s].schema
            if(dataform.projectConfig.schemaSuffix != "") { ref += "_" + dataform.projectConfig.schemaSuffix }
            ref += "." + sources[s].name + "` "
        }
    }
    return "`" + name + "` "
}

module.exports = ref;