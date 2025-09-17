/*config*/
let {ref, getRefs, join, ifNull, ifSource, orSource} = require("../../sources")
let query = `

SELECT
  submission_id,
  buy_car_brand,
  buy_car_model,
  buy_car_type,
  buy_car_engine,
  buy_car_price,
  buy_car_extra_trade_in_value,
  buy_car_deal_title,
  buy_car_deal_value,
  metallic_lak,
  trade_in_license_plate,
  trade_in_mileage,
  trade_in_appraisal,
  trade_in_make,
  trade_in_fuel,
  trade_in_model,
  trade_in_trim,
  trade_in_transmission,
  trade_in_build_year,
  initials,
  first_name,
  infix,
  last_name,
  phone,
  postal_code,
  house_number,
  house_number_addition,
  street,
  city,
  terms,
  dealer_id,
  dealer_name,
  dealer_zipcode,
  dealer_street,
  dealer_city,
  dealer_phone_number,
  dealer_email,
  external_id,
  trade_in_trade_worth,
  trade_in_sell_worth,
  trade_in_trade_in_worth,
  trade_in_new_worth,
  trade_in_offer,
  trade_in_options,
  created_at,
  updated_at,
  status_id,
  status_submission_id,
  status_status,
  status_created_at,
  status_updated_at,
  taxation_date,
  email,
  LEFleadID,
  DATE(created_at) AS created_at_date,
  ${ifSource(["stg_openRdwData", "lef_leads"], `datum_tenaamstelling,
IF
  (DATE(datum_tenaamstelling) >= DATE(created_at), 1,0) AS verkocht,`)}
  DATE(lef.aangemaaktDatum) AS aangemaaktDatum,
  ${ifSource("stg_openRdwData", `IF(
    heeftOrder = "true" AND DATE(datum_tenaamstelling) < DATE(created_at), 1,0) AS in_aflevering,
  IF(
    DATE(datum_tenaamstelling) >= DATE(created_at) AND (lef.heeftOrder = "false" OR lef.heeftOrder IS NULL), 1,0
  ) AS elders_verkocht,
  IF(lef.heeftOrder = 'true', 1,0) AS heeft_order,
  IF(lef.heeftOfferte = 'true',1,0) AS heeft_offerte`)}
  ${ifSource("stg_ga4_events_sessies", `
  session_campaign,
  session_source_medium,
  kanaal,
  `)}
FROM (
  SELECT
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
    kenteken)
ON
  REPLACE(trade_in_license_plate, "-", "") = kenteken`)} rdw
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
WHERE
  rank = 1`)}
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
ON submission_id_otm = submission_id`
)}

`
let refs = getRefs()
module.exports = {query, refs}
