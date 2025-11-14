module.exports = `
--CREATE OR REPLACE FUNCTION \`pk-datalake-van-mossel-groep.rawdata.clean_email\`(raw_input STRING) RETURNS STRING AS (
(
    -- === BYPASS: skip de hele functie als er een spatie is zonder e-mailadres tussen <> ===
    CASE
      WHEN REGEXP_CONTAINS(raw_input, r'\s')
           AND NOT REGEXP_CONTAINS(raw_input, r'<[^>]+@[^\s>]+\.[A-Za-z]{2,}>')
        THEN raw_input
      ELSE (
        WITH base AS (
          SELECT COALESCE(
            REGEXP_EXTRACT(raw_input, r'<([^>]+)>'),
            raw_input
          ) AS email_input
        ),

        emails AS (
          SELECT
            TRANSLATE(
              REGEXP_REPLACE(REGEXP_REPLACE(email_input, r'[#!$^&*\(\)\[\]\{\}\:\;\"\'\<\,\>\?\/]', ""), r'([.\-%+_@]){2,}', r'\1'),
              'ÁÀÂÄáàâäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÖóòôöÚÙÛÜúùûüÑñÇç',
              'AAAAaaaaEEEEeeeeIIIIiiiiOOOOooooUUUUuuuuNnCc'
            ) AS email
          FROM base
        ),

        split_email AS (
          SELECT
            REGEXP_EXTRACT(email, r'([a-zA-Z0-9._%+-]+)@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}') AS first,
            REGEXP_EXTRACT(email, r'[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+)\.[a-zA-Z]{2,}') AS middle,
            REGEXP_EXTRACT(email, r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.([a-zA-Z]{2,})') AS last,
            email
          FROM emails
        ),

        trimmed AS (
          SELECT
            REGEXP_REPLACE(LOWER(first), r'^[._%+-]+|[._%+-]+$', '') AS first,
            REGEXP_REPLACE(LOWER(middle), r'^[.-]+|[.-]+$', '') AS middle,
            CASE
              WHEN REGEXP_CONTAINS(LOWER(last), 'com') THEN 'com'
              WHEN REGEXP_CONTAINS(LOWER(last), 'nl') THEN 'nl'
              WHEN REGEXP_CONTAINS(LOWER(last), 'c') AND REGEXP_CONTAINS(LOWER(last), 'o') AND REGEXP_CONTAINS(LOWER(last), 'm') THEN 'com'
              WHEN REGEXP_CONTAINS(LOWER(last), 'n') AND REGEXP_CONTAINS(LOWER(last), 'l') THEN 'nl'
              WHEN REGEXP_CONTAINS(LOWER(last), 'c') OR REGEXP_CONTAINS(LOWER(last), 'o') OR REGEXP_CONTAINS(LOWER(last), 'm') THEN 'com'
              WHEN REGEXP_CONTAINS(LOWER(last), 'n') OR REGEXP_CONTAINS(LOWER(last), 'l') THEN 'nl'
              ELSE LOWER(last)
            END AS last,
            email
          FROM split_email
        )

        SELECT
          CASE
            WHEN first IS NULL OR middle IS NULL OR last IS NULL THEN email
            ELSE CONCAT(first, '@', middle, '.', last)
          END
        FROM trimmed
      )
    END
  )
--);
`;