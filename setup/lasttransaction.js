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
        CREATE SCHEMA IF NOT EXISTS \`${dataform.projectConfig.defaultDatabase}.df_rawdata_views${pk.schemaSuffix(config)}\` OPTIONS(location="${dataform.projectConfig.defaultLocation}");
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
                MAX(cd.PAYLOAD) AS PAYLOAD,
                MAX(cd.ACTION) AS ACTION,
                MAX(cd.RECEIVEDON) AS RECEIVEDON,
                cd.SCHEMA,
                cd.PRIMARYFIELDHASH,
                cd.ALIAS,
                cd.account,
                
            FROM ${ref(refVal.schema, refVal.name)} cd
            JOIN( 
                SELECT
                    SCHEMA,
                    PRIMARYFIELDHASH,
                    MAX(RECEIVEDON) AS max_receivedon
                FROM ${ref(refVal.schema, refVal.name)}
                GROUP BY SCHEMA, PRIMARYFIELDHASH
            ) ld
              ON cd.SCHEMA = ld.SCHEMA
             AND cd.PRIMARYFIELDHASH = ld.PRIMARYFIELDHASH
             AND cd.RECEIVEDON = ld.max_receivedon
            WHERE cd.ACTION != 'delete'
            GROUP BY cd.SCHEMA, cd.PRIMARYFIELDHASH, cd.account, cd.alias
        
        );
        END;
        END;  
    `
}

module.exports = lasttransaction;