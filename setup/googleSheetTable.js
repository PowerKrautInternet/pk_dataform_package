let pk = require("../sources")
let ref = pk.ref

function googleSheetTable (refVal) {
    if(typeof refVal.name == "undefined") {return "//ERROR: googleSheetTable.js"}
    let name = refVal.alias ?? refVal.name
    let config = {
        "type": "table",
        "schema": "df_googlesheets_tables"
    }
    pk.addSource({
        "name": refVal.name,
        "config": config
    });
    return `BEGIN
  CREATE SCHEMA IF NOT EXISTS \`${dataform.projectConfig.defaultDatabase}.df_googlesheets_tables${pk.schemaSuffix(config)}\` OPTIONS(location="EU");
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
  FROM \`${dataform.projectConfig.defaultDatabase}.df_googlesheets_tables${pk.schemaSuffix(config)}.INFORMATION_SCHEMA.TABLES\`
  WHERE table_name = '${name}'
);
          IF dataform_table_type IS NOT NULL THEN
      IF dataform_table_type = 'VIEW' THEN DROP VIEW IF EXISTS \`${dataform.projectConfig.defaultDatabase}.df_googlesheets_tables${pk.schemaSuffix(config)}.${name}\`;
ELSEIF dataform_table_type = 'MATERIALIZED VIEW' THEN DROP MATERIALIZED VIEW IF EXISTS \`${dataform.projectConfig.defaultDatabase}.df_googlesheets_tables${pk.schemaSuffix(config)}.${name}\`;
END IF;
    END IF;
      BEGIN
        
            CREATE OR REPLACE TABLE \`${dataform.projectConfig.defaultDatabase}.df_googlesheets_tables${pk.schemaSuffix(config)}.${name}\`
    
    
    OPTIONS()
    AS (
      

SELECT
*
FROM
${ref("googleSheets", refVal.name)}
    );
        
      END;
    END;`
}


module.exports = googleSheetTable;