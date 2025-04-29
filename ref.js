const pk = require("./index");
let sources = pk.getSources();

function ref(name) {
    for(let s in sources) {
        if(s.name == name){
            return "`" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "
        }
    }
    return null
}

module.exports = ref;