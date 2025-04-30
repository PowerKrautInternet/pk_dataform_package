let ref = require("../ref.js")
module.exports = `
    
    
 SELECT 
 kanaal, 
 categorie as conversie_mapping,
 CAST(target as INT64) as month_target,
 date as record_datum, 
 CAST(target / ARRAY_LENGTH(dates) as INT64) as day_target

 FROM
 (  
  SELECT 
    kanaal, 
    categorie,
    PARSE_DATE('%Y%m%d', maand) as maand,
    TARGET,
    GENERATE_DATE_ARRAY(
      PARSE_DATE('%Y%m%d', maand),
      LAST_DAY(PARSE_DATE('%Y%m%d', maand), MONTH)
    ) as dates

  FROM
  (  
    SELECT       
      [ STRUCT('20250101'as maand,
          jan AS TARGET),
          STRUCT('20250201'as maand,
          feb AS TARGET),
          STRUCT('20250301'as maand,
          mrt AS TARGET),
          STRUCT('20250401'as maand,
          apr AS TARGET),
          STRUCT('20250501'as maand,
          mei AS TARGET),
          STRUCT('20250601'as maand,
          jun AS TARGET),
          STRUCT('20250701'as maand,
          jul AS TARGET),
          STRUCT('20250801'as maand,
          aug AS TARGET),
          STRUCT('20250901'as maand,
          sep AS TARGET),
          STRUCT('20251001'as maand,
          okt AS TARGET),
          STRUCT('20251101'as maand,
          nov AS TARGET),
          STRUCT('20251201'as maand,
          dec AS TARGET)
        ] AS ROW,
        kanaal,
        categorie

    FROM ${ref("gs_conversie_targets_nieuw")} 
  ) as conversie_mapping
  CROSS JOIN
    UNNEST(conversie_mapping.row) AS r
 ) as date_mapping
 CROSS JOIN
  UNNEST(dates) as date
    
    `