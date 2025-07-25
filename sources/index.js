let sources = [] //TODO hier een import voor maken ipv een require

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
        varsource.noSuffix = varsource.noSuffix ?? false
    } else {
        varsource.noSuffix = varsource.noSuffix ?? true
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

//used in the declarations.js
function setSources(varSource){
    sources = [];
    for(let s in varSource){
        let v = varSource[s];
        v["database"] = typeof varSource[s].database !== "undefined" ? varSource[s].database : dataform.projectConfig.defaultDatabase;

        // Zorgt ervoor dat elke publisher in de array:
        // - een geldig 'name'-veld bevat (anders wordt deze uitgefilterd)
        // - altijd een 'recency'-veld heeft; standaard op true indien niet opgegeven
        // - altijd een 'freshnessDays'-veld heeft; als deze niet gevuld wordt door de publisher dan wordt deze o.b.v. de producer gevuld en anders standaard op 1 gezet.
        v.publishers = (v.publishers ?? [])
            .filter(publisher => !!publisher.name)
            .map(publisher => ({
                name: publisher.name,
                recency: publisher.recency ?? true,
                freshnessDays: publisher.freshnessDays ?? v.freshnessDays ?? 1
            }));

        v["noSuffix"] = true;
        v["declaredSource"] = true;
        sources.push(v);
    }
}

function addSuffix(schema) {
    if (!schema.startsWith("ads_") && !schema.startsWith("analytics_") && schema !== "rawdata" && schema !== "googleSheets" && dataform.projectConfig.schemaSuffix !== "" && typeof dataform.projectConfig.schemaSuffix !== "undefined") {
        return schema+"_"+dataform.projectConfig.schemaSuffix
    }
    return schema
}

function ref(p1, p2, ifSource) {

    p2 = (typeof p2 == 'undefined') ? "" : p2
    let sources = getSources();
    let ref = [];
    let NrFound = 0;
    for(let s in sources) {
        if( //if the ref has only one parameter it has to be the name, when there are 2 parameter the second wil be the name. (name is interchangable with alias)
            (p2 === "" && (sources[s].alias === p1 || (sources[s].name.replace(/_[0-9]+$/g, "") === p1 && ( typeof sources[s].alias == 'undefined' || sources[s].name.startsWith('ads_') || sources[s].name === "events_*" ) ) ) )
            ||
            (p2 !== "" && (sources[s].alias === p2 || (sources[s].name.replace(/_[0-9]+$/g, "") === p2 && (typeof sources[s].alias == 'undefined' || sources[s].name.startsWith('ads_') || sources[s].name === "events_*" || sources[s].name.endsWith("Producer")) ) ) && sources[s].schema === p1)
        ){
            let r = {}
            r.schema = sources[s].schema
            r.alias = sources[s].alias ? '"' + sources[s].alias + '"' : null;
            r.name = sources[s].name ?? ""
            r.query = "`" + sources[s].database + "." + sources[s].schema
            r.account = typeof sources[s].account !== "undefined" ? "'" + sources[s].account + "'" : null;
            r.noSuffix = sources[s].noSuffix ?? null;
            r.declaredSource = sources[s].declaredSource ?? false;
            //voeg een suffix voor development toe. Alleen toevoegen als het niet om brondata gaat (gedefineerd als rawdata of googleSheets)
            if(!(sources[s].noSuffix ?? false) && !sources[s].schema.startsWith("analytics_") && sources[s].schema !== "rawdata" && sources[s].schema !== "googleSheets" && dataform.projectConfig.schemaSuffix !== "" && typeof dataform.projectConfig.schemaSuffix !== "undefined") { r.query += "_" + dataform.projectConfig.schemaSuffix  }
            r.query += "." + sources[s].name + "` "
            ref.push(r)
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
        let refQuery = "--Combining multiple ref results\n"
        for (let r in ref) {
            if (r > 0) {
                refQuery += "UNION ALL";
            } else {
                refQuery += "("
            }
            refQuery += '\nSELECT *, '
            refQuery += getTypeSource(ref[r]) !== "NONE" ? (ref[r].alias ?? "NULL") + " as alias," : ""
            refQuery += `${ref[r].declaredSource ? (ref[r].account ?? "NULL") + " as account," : ""} `
            refQuery += " FROM \n" + ref[r].query;
        }
        refQuery +=" \n)"
        return refQuery
    } else if(p2 && !ifSource) {
        refs.push({
            "database": dataform.projectConfig.defaultDatabase,
            "schema": p1,
            "name": p2
        })
    }

    if(!ifSource) {
        //If none is found the following will try and give an estimated source with default values
        let refQuery = ""
        refQuery += "`" + dataform.projectConfig.defaultDatabase + "."
        if (typeof p2 == "undefined") {
            refQuery += addSuffix(dataform.projectConfig.defaultSchema) + "." + p1
        } else {
            refQuery += addSuffix(p1) + "." + p2
        }
        refQuery += "` "
        return refQuery
    }

    let v = sources.map((s) => s.alias + " " + s.name + " " + s.schema );
    return "NOT FOUND " + dataform.projectConfig.defaultDatabase + "." + p1 + "." + p2 + "\n list:" + v;
}

function getRefs(){//getAndClearRef
    let dependencies = refs;
    dependencies.push("setup_operations")
    refs = []
    return dependencies
}

function schemaSuffix(source) {
    if(source.schema !== "rawdata" && source.schema !== "googleSheets" && dataform.projectConfig.schemaSuffix !== "" && typeof dataform.projectConfig.schemaSuffix !== "undefined") { return "_" + dataform.projectConfig.schemaSuffix } else {return ""}
}

function crm_id(sourceName) {
    for(let s in sources) {
        if(sources[s].alias === sourceName && typeof sources[s].crm_id != "undefined") {
            return '"'+ sources[s].crm_id + '"';
        }
    }
    return "ERROR: crm_id is undefined for " + sourceName
}

function join(joinType, schemaOrName, nameOrJoin, join) {
    let source;
    let nameSource;
    if(typeof  join == "undefined") {
        source = ref(schemaOrName, "", true);
        nameSource = schemaOrName;
        join = nameOrJoin;
    } else {
        source = ref(schemaOrName, nameOrJoin, true);
        nameSource = nameOrJoin;
    }

    return !source.startsWith("NOT FOUND") ? `${joinType} ${source} ${join} \n` : "--No " + nameSource + "\n"
}

function ifNull(values, alias = ""){
    let ifnull = ""
    let valueQuery = ""
    let count = 0
    if(Array.isArray(values)) {
        for (let s in values) {
            if(!values[s].startsWith("/* NOT FOUND //")) {
                if (count !== 0) {
                    ifnull += "IFNULL("
                    valueQuery += ",";
                }
                valueQuery += values[s]
                if (count !== 0) {
                    valueQuery += ")";
                }
                count++;
            }
        }
    }
    if(count !== 0){
        return ifnull + valueQuery + " " + alias;
    }
    return "";
}

function ifSource(name, query){
    if(Array.isArray(name)) {
        for(let s in name) {
            if (ref(name, "", true).startsWith("NOT FOUND")) {
                return "/* NOT FOUND // " + query + "*/";
            }
        }
    } else {
        if (ref(name, "", true).startsWith("NOT FOUND")) {
            return "/* NOT FOUND // " + query + "*/"
        }
    }
    return query;
}

/**
 * @brief Bepaalt het type gegevensbron op basis van de naam of alias van de bron.
 *
 * Deze functie inspecteert het `alias` of `name` attribuut van een bronobject en
 * classificeert de bron in een van de bekende types of "NONE" als geen match wordt gevonden.
 *
 * @param {Object} source - Een object dat informatie bevat over een gegevensbron.
 * @param {string} [source.alias] - Een optionele alias voor de bron.
 * @param {string} [source.name] - De naam van de bron.
 * Wanneer Alias null is, wordt de name gepakt.
 *
 * @returns {string} Het type van de gegevensbron. Mogelijke waarden:
 *   - "googleAds"
 *   - "dataProducer"
 *   - "GA4"
 *   - "DV360"
 *   - "google_search_console"
 *   - "NONE" (standaardwaarde als er geen match is)
 */
function getTypeSource(source){
    let type = "NONE";
    let name = source.alias ?? source.name ?? "";
    if (name.startsWith("ads_AdGroup") || name.startsWith("ads_AssetGroup") || name.startsWith("ads_Campaign")) type = "googleAds"
    else if (name.endsWith("DataProducer")) type = "dataProducer"
    else if (name === "events_*" || name === "events") type = "GA4"
    else if (name.startsWith("Dagelijkse_BQ_export_-_") || name.startsWith("Dagelijkse_BQ_Export_-_")) type = "DV360"
    else if (name === "searchdata_url_impression") type = "google_search_console"
    return type
}
//TODO support/queryhelpers
module.exports = { addSource, setSources, getSources, ref, getRefs, schemaSuffix, crm_id, join, ifNull, ifSource, getTypeSource};
