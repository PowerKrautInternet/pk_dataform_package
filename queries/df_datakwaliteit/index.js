let sources = require("../../sources").getSources();
let {getTypeSource} = require("../../sources");

function dk_maxReceivedon(extraSelect = "", extraSource = "", extraWhere = "", extraGroupBy = "") {
    let query = 'SELECT max(max_receivedon) as max_receivedon, max(recency_check) as recency_check, key1, bron FROM(';
    let rowNr = 0;
    for (let s in sources) {
        let type = getTypeSource(sources[s]);
        let key1 = sources[s].key1 ?? "$.DTCMEDIA_CRM_ID" //pk_crm_id meot geimplementeerd worden
        if ((sources[s].recency !== "false" && !sources[s].recency === false) || typeof sources[s].recency == "undefined") {
            //for each data source
            let name = sources[s].name ?? "";
            if (name.endsWith("DataProducer")) {
                if (rowNr > 0) {
                    query += "\nUNION ALL\n\n"
                }

                query += "SELECT bron, key1, max_receivedon, recency_check\nFROM (\nSELECT \n\tIF(MAX_RECEIVEDON >= CURRENT_DATE()-"
                if (sources[s].freshnessDays == undefined) {
                    query += 1
                } else {
                    query += sources[s].freshnessDays
                }
                query += ", NULL, 1) AS RECENCY_CHECK, *"
                if (extraSelect != "") {
                    query += ", "
                }
                query += extraSelect
                query += " \n\nFROM ( "

                //SELECT ...
                query += "\n\tSELECT "
                query += "\n\tDATE(MAX(DATE_ADD(RECEIVEDON, INTERVAL 2 HOUR))) AS MAX_RECEIVEDON, "     //MAX_RECEIVEDON
                query += "'" + sources[s].name + "' AS BRON, "      //BRON

                //KEY1 ...
                    query += `JSON_VALUE(PAYLOAD, '${key1}') AS KEY1 `

                //FROM ... database . schema . name
                query += "\n\n\tFROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "

                //WHERE ... CRMID
                if (sources[s].crm_id != undefined) {
                    query += "\nWHERE "
                    query += "JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') IN ('"
                    if (Array.isArray(sources[s].crm_id)) {
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

                query += "\n))\n"
                rowNr += 1
            }
            else if (type === "GA4") {
                if (rowNr > 0) {
                    query += "\nUNION ALL\n\n"
                }
                query += "SELECT bron, key1, max_receivedon, recency_check\nFROM (\nSELECT \n\tIF(MAX_RECEIVEDON >= CURRENT_DATE()-"
                if (typeof sources[s].freshnessDays == "undefined") {
                    query += 1
                } else {
                    query += sources[s].freshnessDays
                }
                query += ", NULL, "

                //if the noWeekend is set the true statement of the recency if is always 0
                if (sources[s].noWeekend == true) {
                    query += "0"
                } else {
                    query += "1"
                }
                query += ") AS RECENCY_CHECK, *"
                if (extraSelect != "") {
                    query += ", "
                }
                query += extraSelect
                query += " \n\nFROM ( "

                //SELECT ...
                query += "\n\tSELECT "
                query += "\n\tDATE(MAX(CAST(PARSE_DATE(\"%Y%m%d\",regexp_replace(CAST(event_date AS STRING), \"-\", \"\")) as datetime))) AS MAX_RECEIVEDON, '"
                query += sources[s].account ?? sources[s].schema
                query += "'  AS KEY1, 'GA4' AS BRON"      //BRON

                //FROM ... database . schema . name
                query += "\n\n\tFROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "

                //GROUP BY ...
                query += "\n\n\tGROUP BY "
                query += "BRON, "
                query += "KEY1"

                query += "\n))\n"
                rowNr += 1
            }
            else if (type !== "NONE"){
                if (rowNr > 0) {
                    query += "\nUNION ALL\n\n"
                }
                query += "SELECT bron, key1, max_receivedon, recency_check\nFROM (\nSELECT \nIF(MAX_RECEIVEDON >= CURRENT_DATE()-"
                query += sources[s].freshnessDays ?? 1;
                query += ", NULL, ";

                //if the noWeekend is set the true statement of the recency if is always 0
                sources[s].noWeekend === true ? query += "0" : query += "1"
                query += ") AS RECENCY_CHECK, *"
                query += " \n\nFROM ( "

                //SELECT ...
                query += "\nSELECT\n "

                //MAX_RECEIVEDON
                query += "MAX("
                if(type === "googleAds"){
                    query += "_LATEST_DATE"
                } else if (type === "DV360"){
                    query += "date"
                } else if (type === "google_search_console"){
                    query += "data_date"
                }
                query += ") AS MAX_RECEIVEDON,\n"

                //KEY1
                if(type === "googleAds"){
                    query += "'" + (sources[s].alias ?? name.split("_")[2]) + "'"
                } else if (type === "DV360"){
                    query += sources[s].alias ?? "'"
                    let names = name.split("_")
                    for(let i = 4; names[i] !== "dv360"; i++){
                            if (i > 4){
                                query += " "
                            }
                           query += names[i]
                    }
                    query += "'"
                } else if (type === "google_search_console"){
                    query += "site_url"
                }
                query += " AS KEY1,\n"

                //BRON
                if(type === "googleAds"){
                    query += "'GoogleAds'";
                } else if (type === "DV360"){
                    query += "'DV360'"
                } else if (type === "google_search_console"){
                    query += "'Google Search Console'"
                }
                query += " AS BRON,\n";

                //FROM
                query += "\n\nFROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` \n\nGROUP BY BRON, KEY1\n))\n"
                rowNr += 1
            } else {
                query += "\n--" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + ": Has not been implemented\n"
            }
        } else {
            query += "\n--" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + ": Recency disabled! \n"
        }
    }
    query += ") GROUP BY BRON, KEY1"
    return query
}

function dk_monitor(){
    let query = '';
    let rowNr = 0;
    for (let s in sources) {
        let type = getTypeSource(sources[s]);
        let name = sources[s].name;
        let key1 = sources[s].key1 ?? "$.DTCMEDIA_CRM_ID"

        //for each data source
        if (type !== "NONE") {
            if (rowNr > 0) { query += "\nUNION ALL\n\n" } // Just to join the sources

            //SELECT ...
            query += "\nSELECT stats.BRON, "

            query += "stats.KEY1"
            query += ", date(stats.RECEIVEDON) as RECEIVEDON, date(MAX(maxdate.MAX_RECEIVEDON)) as MAX_RECEIVEDON, MAX(RECENCY_CHECK) as RECENCY_CHECK, "
            query += "COUNT(*) as COUNT, SUM(IF(ACTION = 'insert', 1, 0)) AS count_insert, SUM(IF(ACTION = 'update', 1, 0)) AS count_update, SUM(IF(ACTION = 'delete', 1, 0)) AS count_delete, "

            //FROM ... database . schema . name AS BRON
            query += "\nFROM (\n"
            if(type === "dataProducer") {
                query += "SELECT PAYLOAD, DATE(date_add(RECEIVEDON,interval 2 hour)) AS RECEIVEDON, ACTION, "
                query += "'" + sources[s].name + "' "      //BRON
            } else if (type === "GA4") {
                query += "SELECT 'insert' AS ACTION, CAST(PARSE_DATE(\"%Y%m%d\",regexp_replace(CAST(event_date AS STRING), \"-\", \"\")) as datetime) AS RECEIVEDON, 'GA4' "
            } else if (type === "googleAds"){
                query += "SELECT 'insert' AS ACTION, _DATA_DATE AS RECEIVEDON, 'GoogleAds' "
            } else if (type === "DV360") {
                query +=  "SELECT 'insert' AS ACTION, date as RECEIVEDON, 'DV360' "
            } else if (type === "google_search_console"){
                query += "SELECT 'insert' AS ACTION, data_date as receivedon, 'Google Search Console' "
            }
            query += " AS BRON, \n"
            //KEY1 ...
            if(type === "dataProducer") {
                    query += `JSON_VALUE(PAYLOAD, '${key1}')`
            } else if (type === "GA4") {
                query += "'"
                query += sources[s].account ?? sources[s].schema
                query += "'"
            } else if( type === "googleAds" ){
                query += "'" + (sources[s].alias ?? name.split("_")[2]) + "'"
            } else if (type === "DV360"){
                query += sources[s].alias ?? "'"
                let names = name.split("_")
                for(let i = 4; names[i] !== "dv360"; i++){
                    if (i > 4){
                        query += " "
                    }
                    query += names[i]
                }
                query += "'"
            } else if (type === "google_search_console"){
                query += "site_url"
            }
            query += " AS KEY1 "

            query += "FROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "`) AS stats"

            //JOIN
            query += "\nLEFT JOIN `" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
            if(dataform.projectConfig.schemaSuffix != "" && typeof dataform.projectConfig.schemaSuffix !== "undefined") { query += "_" + dataform.projectConfig.schemaSuffix }
            query += ".dk_maxReceivedon` as maxdate ON "

            query += "stats.BRON = maxdate.BRON AND stats.RECEIVEDON = maxdate.MAX_RECEIVEDON "

            //KEY1 ...
                query += "AND "
                query += "IFNULL(stats.KEY1, '') = IFNULL(maxdate.KEY1, '') "

            //WHERE ... CRMID
            if(sources[s].crm_id != undefined) {
                query += "\nWHERE JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') = '" + sources[s].crm_id + "' "
            }

            query += "GROUP BY "
            query += "BRON, "
            query += "KEY1, "
            query += "RECEIVEDON"

            query += "\n"
            rowNr += 1
        }
    }
    return query
}

function dk_healthRapport() {
    let query = ""

    query += "SELECT CURRENT_DATE() AS DATE, * "

    query += "FROM "
    query += "`" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
    if(dataform.projectConfig.schemaSuffix != "" && typeof dataform.projectConfig.schemaSuffix !== "undefined") { query += "_" + dataform.projectConfig.schemaSuffix }
    query += ".dk_monitor` WHERE MAX_RECEIVEDON IS NOT NULL"

    return query;
}

function dk_errormessages() {
    let query = ""

    query += "SELECT if(alerts.issues_found is null, 'all good', ERROR(FORMAT('ATTENTION: Data has potential quality issues: %t. ', stringify_alert_list))) AS stringify_alert_list FROM ( SELECT array_to_string ( array_agg ( alert IGNORE NULLS ), '; ' ) as stringify_alert_list, array_length(array_agg(alert IGNORE NULLS)) as issues_found from ( select if(recency_check = 1,CONCAT(bron, ':', key1, '(', date_diff(DATE, MAX_RECEIVEDON, DAY), ' days old)' ), NULL) as alert from "
    query += "`" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
    if(dataform.projectConfig.schemaSuffix != "" && typeof dataform.projectConfig.schemaSuffix !== "undefined") { query += "_" + dataform.projectConfig.schemaSuffix }
    query += ".dk_healthRapport` WHERE DATE = CURRENT_DATE() "
    query += " ) as row_conditions ) as alerts"

    return query;
}

module.exports = {dk_maxReceivedon , dk_monitor, dk_healthRapport, dk_errormessages}