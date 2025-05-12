let pk = require("../setup")
let ref = pk.ref

function lasstransaction (refVal) {
    return `
    
        BEGIN
  CREATE SCHEMA IF NOT EXISTS \`pk-datalake-apoint.df_rawdata_views_dev\` OPTIONS(location="EU");
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
  FROM \`pk-datalake-apoint.df_rawdata_views_dev.INFORMATION_SCHEMA.TABLES\`
  WHERE table_name = '${ref(refVal)}'
);
          IF dataform_table_type IS NOT NULL THEN
      IF dataform_table_type = 'BASE TABLE' THEN DROP TABLE IF EXISTS \`pk-datalake-apoint.df_rawdata_views_dev.${ref(refVal)}\`;
ELSEIF dataform_table_type = 'MATERIALIZED VIEW' THEN DROP MATERIALIZED VIEW IF EXISTS \`pk-datalake-apoint.df_rawdata_views_dev.${ref(refVal)}\`;
END IF;
    END IF;
      BEGIN
        
            CREATE OR REPLACE VIEW \`pk-datalake-apoint.df_rawdata_views_dev.${ref(refVal)}\`
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
    \`rawdata.${ref(refVal)}\` AS FIRST
  WHERE
    RECEIVEDON = (
    SELECT
      MAX(RECEIVEDON)
    FROM
      \`rawdata.${ref(refVal)}\` AS second
    WHERE
      first.schema = second.schema
      AND first.PRIMARYFIELDHASH = second.PRIMARYFIELDHASH )
  GROUP BY
    SCHEMA,
    PRIMARYFIELDHASH )
WHERE
  action != 'delete'

    );
        
      END;
    END;
        
    `
}

module.exports = lasstransaction;