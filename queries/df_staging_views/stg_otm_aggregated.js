/*config*/
let {ref, getRefs, join, ifNull, ifSource, orSource} = require("../../sources")
let query = `

SELECT
  account,
  "OTM" as bron,
  submission_id AS submission_id_otm,
  buy_car_brand AS buy_car_brand_otm,
  buy_car_model AS buy_car_model_otm,
  buy_car_type AS buy_car_type_otm,
  buy_car_engine AS buy_car_engine_otm,
  buy_car_price AS buy_car_price_otm,
  buy_car_extra_trade_in_value AS buy_car_extra_trade_in_value_otm,
  buy_car_deal_title AS buy_car_deal_title_otm,
  buy_car_deal_value AS buy_car_deal_value_otm,
  metallic_lak AS metallic_lak_otm,
  trade_in_license_plate AS trade_in_license_plate_otm,
  trade_in_mileage AS trade_in_mileage_otm,
  trade_in_appraisal AS trade_in_appraisal_otm,
  trade_in_make AS trade_in_make_otm,
  trade_in_fuel AS trade_in_fuel_otm,
  trade_in_model AS trade_in_model_otm,
  trade_in_trim AS trade_in_trim_otm,
  trade_in_transmission AS trade_in_transmission_otm,
  trade_in_build_year AS trade_in_build_year_otm,
  terms AS terms_otm,
  dealer_id AS dealer_id_otm,
  dealer_name AS dealer_name_otm,
  external_id AS external_id_otm,
  trade_in_trade_worth AS trade_in_trade_worth_otm,
  trade_in_sell_worth AS trade_in_sell_worth_otm,
  trade_in_trade_in_worth AS trade_in_trade_in_worth_otm,
  trade_in_new_worth AS trade_in_new_worth_otm,
  trade_in_offer AS trade_in_offer_otm,
  trade_in_options AS trade_in_options_otm,
  created_at AS created_at_otm,
  updated_at AS updated_at_otm,
  status_id AS status_id_otm,
  status_submission_id AS status_submission_id_otm,
  status_status AS status_status_otm,
  status_created_at AS status_created_at_otm,
  status_updated_at AS status_updated_at_otm,
  taxation_date AS taxation_date_otm,
  DATE(created_at) AS created_at_date_otm,
  ${ifSource("stg_openRdwData", `
    datum_tenaamstelling AS datum_tenaamstelling_otm,
    IF(DATE(datum_tenaamstelling) >= DATE(created_at), 1,0) AS verkocht_otm,`)}
  ${ifSource(["stg_openRdwData", "lef_leads"], `
    IF(heeftOrder = "true" AND DATE(datum_tenaamstelling) < DATE(created_at), 1,0) AS in_aflevering_otm,
    IF(DATE(datum_tenaamstelling) >= DATE(created_at) AND (lef.heeftOrder = "false" OR lef.heeftOrder IS NULL), 1,0) AS elders_verkocht_otm,`)}
    ${ifSource("lef_leads", `
    IF(lef.heeftOrder = 'true', 1,0) AS heeft_order_otm,
    IF(lef.heeftOfferte = 'true',1,0) AS heeft_offerte_otm,
    LEFleadID AS LEFleadID_otm,
    DATE(lef.aangemaaktDatum) AS aangemaaktDatum_otm,`)}
  ${ifSource("stg_ga4_events_sessies", `
    session_campaign AS session_campaign_otm,
    session_source_medium AS session_source_medium_otm,
    kanaal AS kanaal_otm,`)}
FROM (
  SELECT
    otm.account AS account,
    submission_id AS submission_id,
    buyCarBrand AS buy_car_brand,
    buyCarModel AS buy_car_model,
    buyCarType AS buy_car_type,
    buyCarEngine AS buy_car_engine,
    buyCarPrice AS buy_car_price,
    buyCarExtraTradeInValue AS buy_car_extra_trade_in_value,
    buyCarDealTitle AS buy_car_deal_title,
    buyCarDealValue AS buy_car_deal_value,
    metallic_lak AS metallic_lak,
    tradeInLicensePlate AS trade_in_license_plate,
    tradeInMileage AS trade_in_mileage,
    tradeInAppraisal AS trade_in_appraisal,
    tradeInMake AS trade_in_make,
    tradeInFuel AS trade_in_fuel,
    tradeInModel AS trade_in_model,
    tradeInTrim AS trade_in_trim,
    tradeInTransmission AS trade_in_transmission,
    tradeInBuildYear AS trade_in_build_year,
    initials AS initials,
    firstName AS first_name,
    infix AS infix,
    lastName AS last_name,
    phone AS phone,
    email AS email,
    postalCode AS postal_code,
    houseNumber AS house_number,
    houseNumberAddition AS house_number_addition,
    street AS street,
    city AS city,
    terms AS terms,
    dealerId AS dealer_id,
    dealerName AS dealer_name,
    dealerZipcode AS dealer_zipcode,
    dealerStreet AS dealer_street,
    dealerCity AS dealer_city,
    dealerPhoneNumber AS dealer_phone_number,
    dealerEmail AS dealer_email,
    externalId AS external_id,
    tradeInTradeWorth AS trade_in_trade_worth,
    tradeInSellWorth AS trade_in_sell_worth,
    tradeInTradeInWorth AS trade_in_trade_in_worth,
    tradeInNewWorth AS trade_in_new_worth,
    tradeInOffer AS trade_in_offer,
    tradeInOptions AS trade_in_options,
    taxation_date AS taxation_date,
    created_at AS created_at,
    updated_at AS updated_at,
    status_id AS status_id,
    status_submission_id AS status_submission_id,
    status_status AS status_status,
    status_created_at AS status_created_at,
    status_updated_at AS status_updated_at,
    ROW_NUMBER() OVER(PARTITION BY submission_id ORDER BY status_updated_at DESC, status_id DESC) AS rank
  FROM
     ${ref("df_rawdata_views", "taxatiemoduleonline")}) otm
  ${join(`LEFT JOIN (SELECT
  kenteken,
    MAX(datum_tenaamstelling) AS datum_tenaamstelling
  FROM`, "stg_openRdwData", 
         `GROUP BY
    kenteken) rdw
ON
  REPLACE(trade_in_license_plate, "-", "") = kenteken`)} 
${join(`LEFT JOIN (
  SELECT
  * EXCEPT(rank)
  FROM(
  SELECT
  huidigKenteken,
  heeftOfferte,
  heeftOrder,
  LEFleadID,
  DATE(aangemaaktDatum) AS aangemaaktDatum,
  ROW_NUMBER() OVER(PARTITION BY REPLACE(huidigKenteken, '-', '') ORDER BY DATE(aangemaaktDatum) DESC) AS rank
  FROM`, "df_rawdata_views", "lef_leads", 
       `WHERE bron = "Taxatie Module Online")
  WHERE rank = 1) lef
ON REPLACE(huidigKenteken, '-', '') = kenteken AND DATE(created_at) = aangemaaktDatum
`)}
${join(`LEFT JOIN (SELECT
MAX(IFNULL(session_campaign, first_user_campaign_name)) as session_campaign,
MAX(session_source_medium) as session_source_medium,
MAX(IFNULL(kanaal,
    CASE
        WHEN regexp_contains(session_source,'dv360') 
        OR regexp_contains(session_medium,'^(.*cpm.*)$') THEN 'DV360'
        WHEN regexp_contains(session_source,'facebook|Facebook|fb|instagram|ig|meta')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|facebookadvertising|Instant_Experience|.*paid.*)$') THEN 'META'
        WHEN regexp_contains(session_source,'linkedin')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'LinkedIn'
        WHEN regexp_contains(session_source,'google|adwords')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Google Ads'
        WHEN regexp_contains(session_source,'bing')
        AND regexp_contains(session_medium,'^(.*cp.*|ppc|.*paid.*)$') THEN 'Microsoft Ads'
        WHEN regexp_contains(session_source,'ActiveCampaign') THEN 'ActiveCampaign'
        ELSE NULL
    END)) AS kanaal,
submission_id_otm
FROM`, "df_staging_tables", "stg_ga4_events_sessies", `
GROUP BY submission_id_otm) AS ga4 
ON CAST(submission_id_otm AS STRING) = submission_id`
)}
WHERE
  rank = 1
`
let refs = getRefs()
module.exports = {query, refs}
