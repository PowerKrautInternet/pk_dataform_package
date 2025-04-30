const pk = require("./index");

function ref(name) {
    let sources = pk.getSources();
    for(let s in sources) {
        if(s.name == name){
            return "`" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "
        }
    }
    return null
}

module.exports = ref;