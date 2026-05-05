let string = `

(
  SELECT
    CASE
      WHEN m IS NULL THEN NULL
      WHEN CHAR_LENGTH(m) > 2 THEN CONCAT(UPPER(SUBSTR(m, 1, 1)), SUBSTR(m, 2))
      ELSE UPPER(m)
    END
  FROM (
    SELECT REGEXP_EXTRACT(
      REGEXP_REPLACE(
        REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(IFNULL(needle, ''), NFD), r'\\p{Mn}+', ''),
        r'[^a-z0-9]+', ' '
      ),
      CONCAT(r'\\b(?:', (
        SELECT STRING_AGG(
          REGEXP_REPLACE(
            REGEXP_REPLACE(NORMALIZE_AND_CASEFOLD(opt, NFD), r'\\p{Mn}+', ''),
            r'[^a-z0-9]+', ' '
          ),
          '|'
          ORDER BY CHAR_LENGTH(opt) DESC
        )
        FROM UNNEST(JSON_VALUE_ARRAY(IFNULL(haystack, '[]'))) AS opt
      ), r')\\b')
    ) AS m
  )
)

`;

module.exports = string
