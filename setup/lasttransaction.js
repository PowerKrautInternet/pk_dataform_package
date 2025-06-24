let pk = require("../sources")
let ref = pk.ref

function lasttransaction (refVal) {
    let config = {
        "type": "view",
        "schema": "df_rawdata_views"
    }
    pk.addSource({
        "name": refVal.name+"_lasttransaction",
        "config": config
    });

    return `
    
        BEGIN
        CREATE SCHEMA IF NOT EXISTS \`${dataform.projectConfig.defaultDatabase}.df_rawdata_views${pk.schemaSuffix(config)}\` OPTIONS(location="EU");
        EXCEPTION WHEN ERROR THEN
        IF NOT CONTAINS_SUBSTR(@@error.message, "already exists: dataset") AND
        NOT CONTAINS_SUBSTR(@@error.message, "too many dataset metadata update operations") AND
        NOT CONTAINS_SUBSTR(@@error.message, "User does not have bigquery.datasets.create permission")
        THEN
        RAISE USING MESSAGE = @@error.message;
        END IF;
        END;
        BEGIN
        DECLARE dataform_table_type DEFAULT (
            SELECT ANY_VALUE(table_type)
            FROM \`${dataform.projectConfig.defaultDatabase}.df_rawdata_views${pk.schemaSuffix(config)}.INFORMATION_SCHEMA.TABLES\`
            WHERE table_name = '${refVal.name}_lasttransaction'
        );
        IF dataform_table_type IS NOT NULL THEN
        IF dataform_table_type = 'BASE TABLE' THEN DROP TABLE IF EXISTS \`${dataform.projectConfig.defaultDatabase + ".df_rawdata_views" + pk.schemaSuffix(refVal) + "." + refVal.name}_lasttransaction\`;
        ELSEIF dataform_table_type = 'MATERIALIZED VIEW' THEN DROP MATERIALIZED VIEW IF EXISTS \`${dataform.projectConfig.defaultDatabase + ".df_rawdata_views" + pk.schemaSuffix(refVal) + "." + refVal.name}_lasttransaction\`;
        END IF;
        END IF;
        BEGIN
        
        CREATE OR REPLACE VIEW \`${dataform.projectConfig.defaultDatabase}.df_rawdata_views${pk.schemaSuffix(config)}.${refVal.name}_lasttransaction\`
        OPTIONS()
        AS (
            SELECT
            *
            FROM (
                SELECT
                MAX(PAYLOAD) AS PAYLOAD,
                MAX(ACTION) AS ACTION,
                MAX(RECEIVEDON) AS RECEIVEDON,
                SCHEMA,
                PRIMARYFIELDHASH
                FROM
                \`${refVal.database + "." + refVal.schema + "." + refVal.name}\` AS FIRST
                WHERE
                RECEIVEDON = (
                    SELECT
                    MAX(RECEIVEDON)
                    FROM
                    \`${refVal.database + "." + refVal.schema + "." + refVal.name}\` AS second
                    WHERE
                    first.schema = second.schema
                    AND first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH 
                )
                GROUP BY
                SCHEMA,
                PRIMARYFIELDHASH 
            )
            WHERE
            action != 'delete'
        );
        END;
        END;  
    `
}

module.exports = lasttransaction;