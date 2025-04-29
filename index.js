let sources = []
let ga4_events = new Query();

function Query(){
    this.name = "";
    this.config = {};
    this.query = "";
}

function setSources(varSource){
    sources = varSource;
    setQuerys();
}

function setQuerys() {
    ga4_events = {
        "name":"ga4_events",
        "config":{
            "type":"view",
            "schema":"df_rawdata"
        },
        "query":set_ga4_events()
    }

    return {"ga4_events": ga4_events};
}

function getLastQuery(extraSelect = "", extraSource = "", extraWhere = "", extraGroupBy = "") {
    let query = '';
    let rowNr = 0;
    for (let s in sources) {
        //for each data source
        if ( sources[s].name.endsWith("DataProducer")) {
            if (rowNr > 0) {
                query += "\nUNION ALL\n\n"
            }

            query += "SELECT \n\tIF(MAX_RECEIVEDON >= CURRENT_DATE()-" 
            if(sources[s].freshnessDays == undefined){
                query += 1
            } else {
                query += sources[s].freshnessDays
            }
            query += ", NULL, "

            //if the noWeekend is set the true statement of the recency if is always 0
            if(sources[s].noWeekend == true){
                query += "0"
            } else {
                query += "1"
            }
            query += ") AS RECENCY_CHECK, *"
            if (extraSelect != ""){
                query += ", "
            }
            query += extraSelect
            query += " \n\nFROM ( "

            //SELECT ...
            query += "\n\tSELECT "
            query +=  "\n\tDATE(MAX(DATE_ADD(RECEIVEDON, INTERVAL 2 HOUR))) AS MAX_RECEIVEDON, "     //MAX_RECEIVEDON
            query += "'" + sources[s].name + "' AS BRON, "      //BRON

            //KEY1 ...
            if(sources[s].key1 != undefined){
                query += "JSON_VALUE(PAYLOAD, '" + sources[s].key1+ "')"
            } else {
                query += "STRING(NULL)"
            }
            query += " AS KEY1 "

            //FROM ... database . schema . name
            query += "\n\n\tFROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "

            //WHERE ... CRMID
            if(sources[s].crm_id != undefined) {
                query += "\nWHERE "
                query += "JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') IN ('"
                if(Array.isArray(sources[s].crm_id)) {
                    query += sources[s].crm_id.join("','")
                } else {
                    query += sources[s].crm_id
                }
                query += "') "
            }

            //GROUP BY ...
            query += "\n\n\tGROUP BY "
            query += "BRON, "
            query += "KEY1"

            query += "\n)\n"
            rowNr += 1
        }
    }    
    return query
}

function getStatsQuery(){
    let query = '';
    let rowNr = 0;
    for (let s in sources) {
        //for each data source
        if ( sources[s].name.endsWith("DataProducer")) {
            if (rowNr > 0) {
                query += "UNION ALL "
            }
            //SELECT ...
            query += "\nSELECT "
            query +=  "stats.BRON, "
            if(sources[s].key1 == undefined){
                query += "MAX("
            }
            query += "stats.KEY1"
            if(sources[s].key1 == undefined){
                query += ") as KEY1"
            }
            query += ", stats.RECEIVEDON, MAX(maxdate.MAX_RECEIVEDON) as MAX_RECEIVEDON, MAX(RECENCY_CHECK) as RECENCY_CHECK, "
            query += "COUNT(PAYLOAD) as COUNT, SUM(IF(ACTION = 'insert', 1, 0)) AS count_insert, SUM(IF(ACTION = 'update', 1, 0)) AS count_update, SUM(IF(ACTION = 'delete', 1, 0)) AS count_delete, "

            //FROM ... database . schema . name
            query += "\nFROM ("
            query += "SELECT PAYLOAD, DATE(RECEIVEDON) AS RECEIVEDON, ACTION, "
            query += "'" + sources[s].name + "' AS BRON, "      //BRON

            //KEY1 ...
            if(sources[s].key1 != undefined){
                query += "JSON_VALUE(PAYLOAD, '" + sources[s].key1+ "')"
            } else {
                query += "STRING(NULL)"
            }
            query += " AS KEY1 "

            query += "FROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "`) AS stats"

            //JOIN
            query += "\nLEFT JOIN `" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
            if(dataform.projectConfig.schemaSuffix != "") { query += "_" + dataform.projectConfig.schemaSuffix }
            query += ".dk_maxReceivedon` as maxdate ON "

            query += "stats.BRON = maxdate.BRON AND date(stats.RECEIVEDON) = maxdate.MAX_RECEIVEDON "

            //KEY1 ...
            if(sources[s].key1 != undefined){
                query += "AND "
                query += "stats.KEY1 = maxdate.KEY1 "
            }

            //WHERE ... CRMID
            if(sources[s].crm_id != undefined) {
                query += "\nWHERE JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') = '" + sources[s].crm_id + "' "            
            }

            query += "GROUP BY "
            query += "BRON, "
            if(sources[s].key1 != undefined){
                query += "KEY1, "
            }
            query += "RECEIVEDON"

            query += "\n"
            rowNr += 1
        }
    }    
    return query
}

function getHealthQuery() {
    let query = ""

    query += "SELECT CURRENT_DATE() AS DATE, * "

    query += "FROM "
    query += "`" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
    if(dataform.projectConfig.schemaSuffix != "") { query += "_" + dataform.projectConfig.schemaSuffix }
    query += ".dk_monitor` WHERE MAX_RECEIVEDON IS NOT NULL"

    return query;
}

function getErrorQuery() {
	let query = ""
	
	query += "SELECT if(alerts.issues_found is null, 'all good', ERROR(FORMAT('ATTENTION: Data has potential quality issues: %t. ', stringify_alert_list))) AS stringify_alert_list FROM ( SELECT array_to_string ( array_agg ( alert IGNORE NULLS ), '; ' ) as stringify_alert_list, array_length(array_agg(alert IGNORE NULLS)) as issues_found from ( select if(recency_check = 1,CONCAT(bron, ':', key1, '(', date_diff(DATE, MAX_RECEIVEDON, DAY), ' days old)' ), NULL) as alert from "
	query += "`" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
    if(dataform.projectConfig.schemaSuffix != "") { query += "_" + dataform.projectConfig.schemaSuffix }
    query += ".dk_healthRapport` WHERE DATE = CURRENT_DATE()"
	query += " ) as row_conditions ) as alerts"
	
	return query;
}

function set_ga4_events() {
    let ga4_select_query = " ( SELECT * FROM "
    let sourceCount = 0;
    for (let s in sources) {
        //for each data source
        if (sources[s].type == "GA4") {
            if(sourceCount > 0){
                ga4_select_query += " UNION ALL SELECT * FROM "
            }
            ga4_select_query += "`" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "
            sourceCount++;
        }
    }
    ga4_select_query += ")"

    let ga4_events = require('./df_rawdata_views/ga4_events.js');
    ga4_events = ga4_events.replace('GA4_BRON', ga4_select_query);
    return ga4_events;
}

module.exports = { setSources, getLastQuery, getStatsQuery, getHealthQuery, getErrorQuery, ga4_events, set_ga4_events, setQuerys};
