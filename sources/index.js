let sources = []

//Posible source keys:
//Database
//Schema
//Name
//Key1 -> publisher JSON value in PAYLOAD for recency alerts. for now only accepts '$.' values
//CRM_ID -> An filter on the CRM_ID in the payload. ($.crm_id)
//type -> An indicator if its an perticular type of data. Implented types:
    //GA4
//noSuffix -> If it is an static source, than it should not have the _dev suffix.

function addSource(varsource) {
    let source = {
        "name": varsource.name,
        "schema": varsource.config.schema,
        "database": dataform.projectConfig.defaultDatabase,
        "noSuffix": "false"
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


module.exports = { addSource, setSources, getSources};
