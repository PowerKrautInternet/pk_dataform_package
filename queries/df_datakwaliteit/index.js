let sources = require("../../sources").getSources();

function dk_maxReceivedon(extraSelect = "", extraSource = "", extraWhere = "", extraGroupBy = "") {
    let query = '';
    let rowNr = 0;
    for (let s in sources) {
        let type = getTypeSource(sources[s]);
        if (sources[s].recency !== "false") {
            //for each data source
            let name = sources[s].name ?? "";
            if (name.endsWith("DataProducer")) {
                if (rowNr > 0) {
                    query += "\nUNION ALL\n\n"
                }

                query += "SELECT \n\tIF(MAX_RECEIVEDON >= CURRENT_DATE()-"
                if (sources[s].freshnessDays == undefined) {
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
                query += "\n\tDATE(MAX(DATE_ADD(RECEIVEDON, INTERVAL 2 HOUR))) AS MAX_RECEIVEDON, "     //MAX_RECEIVEDON
                query += "'" + sources[s].name + "' AS BRON, "      //BRON

                //KEY1 ...
                if (sources[s].key1 != undefined) {
                    query += "JSON_VALUE(PAYLOAD, '" + sources[s].key1 + "')"
                } else {
                    query += "STRING(NULL)"
                }
                query += " AS KEY1 "

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

                query += "\n)\n"
                rowNr += 1
            }
            else if (name === "events_*") {
                if (rowNr > 0) {
                    query += "\nUNION ALL\n\n"
                }
                query += "SELECT \n\tIF(MAX_RECEIVEDON >= CURRENT_DATE()-"
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
                query += "\n\tDATE(MAX(PARSE_DATE(\"%Y%m%d\",event_date))) AS MAX_RECEIVEDON, '"
                query += sources[s].alias ?? sources[s].schema
                query += "'  AS KEY1, 'GA4' AS BRON"      //BRON

                //FROM ... database . schema . name
                query += "\n\n\tFROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` "

                //GROUP BY ...
                query += "\n\n\tGROUP BY "
                query += "BRON, "
                query += "KEY1"

                query += "\n)\n"
                rowNr += 1
            }
            else if (type !== "NONE"){
                if (rowNr > 0) query += "\nUNION ALL\n";
                query += "SELECT \nIF(MAX_RECEIVEDON >= CURRENT_DATE()-";
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
                }
                query += ") AS MAX_RECEIVEDON,\n"

                //KEY1
                if(type === "googleAds"){
                    query += sources[s].alias ?? "'" + name.split("_")[2] + "'"
                } else if (type === "DV360"){
                    query += sources[s].alias ?? "'" + name.split("_")[4] + "'"
                }
                query += " AS KEY1,\n"

                //BRON
                if(type === "googleAds"){
                    query += "'GoogleAds'";
                } else if (type === "DV360"){
                    query += "'DV360'"
                }
                query += " AS BRON,\n";

                //FROM
                query += "\n\nFROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "` \n\nGROUP BY BRON, KEY1\n)\n"
                rowNr += 1
            }
        }
    }
    return query
}

function getTypeSource(source){
    let type = "NONE";
    let name = source.name ?? "";
    if (name.startsWith("ads_AdGroup_")) type = "googleAds"
    else if (name.endsWith("DataProducer")) type = "dataProducer"
    else if (name === "events_*") type = "GA4"
    else if (name.startsWith("Dagelijkse_BQ_export_-_")) type = "DV360"
    return type
}

function dk_monitor(){
    let query = '';
    let rowNr = 0;
    for (let s in sources) {
        let type = getTypeSource(sources[s]);
        let name = sources[s].name;

        //for each data source
        if (type !== "NONE") {
            if (rowNr > 0) { query += "\nUNION ALL\n\n" } // Just to join the sources

            //SELECT ...
            query += "\nSELECT stats.BRON, "

            if(sources[s].key1 === undefined && type === "dataProducer") {
                query += "MAX("
            }
            query += "stats.KEY1"
            if(sources[s].key1 === undefined && type === "dataProducer"){
                query += ") as KEY1"
            }
            query += ", stats.RECEIVEDON, MAX(maxdate.MAX_RECEIVEDON) as MAX_RECEIVEDON, MAX(RECENCY_CHECK) as RECENCY_CHECK, "
            query += "COUNT(*) as COUNT, SUM(IF(ACTION = 'insert', 1, 0)) AS count_insert, SUM(IF(ACTION = 'update', 1, 0)) AS count_update, SUM(IF(ACTION = 'delete', 1, 0)) AS count_delete, "

            //FROM ... database . schema . name AS BRON
            query += "\nFROM ("
            if(type === "dataProducer") {
                query += "SELECT PAYLOAD, DATE(RECEIVEDON) AS RECEIVEDON, ACTION, "
                query += "'" + sources[s].name + "' "      //BRON
            } else if (type === "GA4") {
                query += "SELECT 'insert' AS ACTION, PARSE_DATE(\"%Y%m%d\",event_date) AS RECEIVEDON, 'GA4' "
            } else if (type === "googleAds"){
                query += "SELECT 'insert' AS ACTION, _DATA_DATE AS RECEIVEDON, 'GoogleAds' "
            } else if (type === "DV360") {
                query +=  "SELECT 'insert' AS ACTION, date as RECEIVEDON, 'DV360' "
            }
            query += " AS BRON, \n"
            //KEY1 ...
            if(type === "dataProducer") {
                if (sources[s].key1 !== undefined) {
                    query += "JSON_VALUE(PAYLOAD, '" + sources[s].key1 + "')"
                } else {
                    query += "STRING(NULL)"
                }
            } else if (type === "GA4") {
                query += "'"
                query += sources[s].alias ?? sources[s].schema
                query += "'"
            } else if( type === "googleAds" ){
                query += sources[s].alias ?? "'" + name.split("_")[2] + "'"
            } else if (type === "DV360") {
                query += sources[s].alias ?? "'" + name.split("_")[4] + "'"
            }
            query += " AS KEY1 "

            query += "FROM `" + sources[s].database + "." + sources[s].schema + "." + sources[s].name + "`) AS stats"

            //JOIN
            query += "\nLEFT JOIN `" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
            if(dataform.projectConfig.schemaSuffix != "") { query += "_" + dataform.projectConfig.schemaSuffix }
            query += ".dk_maxReceivedon` as maxdate ON "

            query += "stats.BRON = maxdate.BRON AND date(stats.RECEIVEDON) = maxdate.MAX_RECEIVEDON "

            //KEY1 ...
            if(sources[s].key1 != undefined || type !== "dataProducer") {
                query += "AND "
                query += "stats.KEY1 = maxdate.KEY1 "
            }

            //WHERE ... CRMID
            if(sources[s].crm_id != undefined) {
                query += "\nWHERE JSON_VALUE(PAYLOAD, '$.DTCMEDIA_CRM_ID') = '" + sources[s].crm_id + "' "
            }

            query += "GROUP BY "
            query += "BRON, "
            if(sources[s].key1 != undefined || type !== "dataProducer") {
                query += "KEY1, "
            }
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
    if(dataform.projectConfig.schemaSuffix != "") { query += "_" + dataform.projectConfig.schemaSuffix }
    query += ".dk_monitor` WHERE MAX_RECEIVEDON IS NOT NULL"

    return query;
}

function dk_errormessages() {
    let query = ""

    query += "SELECT if(alerts.issues_found is null, 'all good', ERROR(FORMAT('ATTENTION: Data has potential quality issues: %t. ', stringify_alert_list))) AS stringify_alert_list FROM ( SELECT array_to_string ( array_agg ( alert IGNORE NULLS ), '; ' ) as stringify_alert_list, array_length(array_agg(alert IGNORE NULLS)) as issues_found from ( select if(recency_check = 1,CONCAT(bron, ':', key1, '(', date_diff(DATE, MAX_RECEIVEDON, DAY), ' days old)' ), NULL) as alert from "
    query += "`" + dataform.projectConfig.defaultDatabase + ".df_datakwaliteit"
    if(dataform.projectConfig.schemaSuffix != "") { query += "_" + dataform.projectConfig.schemaSuffix }
    query += ".dk_healthRapport` WHERE DATE = CURRENT_DATE()"
    query += " ) as row_conditions ) as alerts"

    return query;
}

module.exports = {dk_maxReceivedon , dk_monitor, dk_healthRapport, dk_errormessages}