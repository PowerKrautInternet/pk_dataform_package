let sources = [];

function setSources(varSource){
    sources = varSource;
}

function maxReceivedon(extraSelect = "", extraSource = "", extraWhere = "", extraGroupBy = "") {
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
            query += ", 0, "

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
                query += "JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') = '" + sources[s].crm_id + "' "
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

function getStats(){
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
                query += ")"
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
            query += "\nLEFT JOIN `" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit_" + dataform.projectConfig.schemaSuffix + ".dk_maxReceivedon` as maxdate ON "

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

function setHealthRapport() {
    let query = ""

    query += "SELECT CURRENT_DATE() AS DATE, * "

    query += "FROM "
    query += "`" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit_" + dataform.projectConfig.schemaSuffix + ".dk_monitor` WHERE MAX_RECEIVEDON IS NOT NULL"

    return query;
}

module.exports = { setSources, maxReceivedon, getStats, setHealthRapport };
