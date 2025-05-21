let sources = []

//Posible source keys:
//Database
//Schema
//Name
//Key1 -> publisher JSON value in PAYLOAD for recency alerts. for now only accepts '$.' values
//CRM_ID -> An filter on the CRM_ID in the payload. ($.crm_id)
//alias -> An indicator if it's a perticular type of data. Implented types:
    //GA4
//noSuffix -> If it is a static source, then it should not have the _dev suffix.

let refs = []

function addSource(varsource) {
    //Maybe this could be shorter, but im not sure a JSON likes to have an boolean assigned in this usecase
    if (varsource.type !== "function") {
        varsource.noSuffix = "false"
    } else {
        varsource.noSuffix = "true"
    }
    let source = {
        "name": varsource.name,
        "schema": varsource.config.schema,
        "database": dataform.projectConfig.defaultDatabase,
        "noSuffix": varsource.noSuffix,
        "type": varsource.type
    }
    sources.push(source);
}

function getSources() {
    return sources;
}

function setSources(varSource){
    sources = [];
    for(let s in varSource){
        let v = varSource[s];
        v["noSuffix"] = "true";
        sources.push(v);
    }
}

function ref(p1, p2) {
    let sources = getSources();
    let ref = []
    let NrFound = 0;
    for(let s in sources) {
        if( //if the ref has only one parameter it has to be the name, when there are 2 parameter the second wil be the name. (name is interchangable with alias)
            (typeof p2 == "undefined" && (sources[s].alias === p1 || (sources[s].name === p1 && typeof sources[s].alias == 'undefined') ) )
            ||
            (typeof p2 != "undefined" && (sources[s].alias === p2 || (sources[s].name === p2 && typeof sources[s].alias == 'undefined') ) && sources[s].schema === p1)
        ){
            ref[NrFound] = "`" + sources[s].database + "." + sources[s].schema
            //voeg een suffix voor development toe. Alleen toevoegen als het niet om brondata gaat (gedefineerd als rawdata of googleSheets)
            if(sources[s].schema !== "rawdata" && sources[s].schema !== "googleSheets" && dataform.projectConfig.schemaSuffix !== "") { ref[NrFound] += "_" + dataform.projectConfig.schemaSuffix }
            ref[NrFound] += "." + sources[s].name + "` "
            if(sources[s].type !== "function") {
                refs.push({
                    "name": sources[s].name,
                    "schema": sources[s].schema,
                    "database": sources[s].database
                })
            } else {
                return "`" + sources[s].database + "`.rawdata." + p1
            }
            NrFound++;
        }
    }


    //if a ref was found than return al the refs that where found in an query that will be implemented in a 'from'
    if(NrFound > 0) {
        let refQuery = ""
        refQuery = "\n(SELECT * FROM \n";
        for (let r in ref) {
            if (r > 0) {
                refQuery += "UNION ALL "
            }
            refQuery += ref[r];
        }
        refQuery +=" \n)"
        return refQuery;
    }

    /*
    //If none is found the following will try and give an estimated source with default values
    ref += "`" + dataform.projectConfig.defaultDatabase + "."
    if(typeof p2 == "undefined") {
        ref += dataform.projectConfig.defaultSchema + "." + p1
        //refs.push({
         //   "name": p1,
         //   "schema": dataform.projectConfig.defaultSchema,
        //    "database": dataform.projectConfig.defaultDatabase
        //})
    } else {
        ref += p1 + "." + p2
        //refs.push({
        //    "name": p2,
       //     "schema": p1,
       //     "database": dataform.projectConfig.defaultDatabase
       // })
    }
    ref += "` "
    */
    return "NOT FOUND" + p1 + p2;
}

function getRefs(){
    let dependencies = refs;
    refs = []
    return dependencies
}

function schemaSuffix(source) {
    if(source.schema !== "rawdata" && source.schema !== "googleSheets" && dataform.projectConfig.schemaSuffix !== "") { return "_" + dataform.projectConfig.schemaSuffix } else {return ""}
}

function crm_id(name) {
    for(let s in sources) {
        if(sources[s].alias === name && typeof sources[s].crm_id != "undefined") {
            return '"'+ sources[s].crm_id + '"';
        }
    }
    return "ERROR: crm_id is undefined for " + name
}

function join(joinType, schemaOrName, nameOrJoin, join) {
    let source;
    let schema;
    let nameSource;
    if(typeof  join == "undefined") {
        source = ref(schemaOrName);
        nameSource = schemaOrName;
        join = nameOrJoin;
    } else {
        source = ref(schemaOrName, nameOrJoin);
        schema = schemaOrName;
        nameSource = nameOrJoin;
    }

    return source.startsWith("NOT FOUND") ? `${joinType} ${source} ${join} \n` : "--No " + nameSource + "\n"
}

function ifNull(values, alias){
    let ifnull = ""
    let valueQuery = ""
    if(Array.isArray(values)) {
        for (let s in values) {
            if(s != 0){
                ifnull += "IFNULL("
                valueQuery += ",";
            }
            valueQuery += values[s] ?? "NULL"
            if(s != 0){
                valueQuery += ")";
            }
        }
    }
    return ifnull + valueQuery;
}

function ifSource(name, query){
    if(!ref(name).startsWith("--No")){
        return query
    }
}

module.exports = { addSource, setSources, getSources, ref, getRefs, schemaSuffix, crm_id, join, ifNull, ifSource};
