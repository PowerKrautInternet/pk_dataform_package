/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

SELECT 
  IFNULL(datum_bericht, deal_eind_datum)                                              AS datum_bericht,
  IFNULL(email_stats.deal_afsluitreden, deals_gemist.deal_afsluitreden)               AS deal_afsluitreden,
  IFNULL(email_stats.deal_vestiging, deals_gemist.deal_vestiging)                     AS deal_vestiging,
  IFNULL(email_stats.gewenst_merk, deals_gemist.gewenst_merk)                         AS gewenst_merk,
  IFNULL(extra_information, deal_source)                                              AS systeem,
  email_stats.* except(datum_bericht, deal_afsluitreden, deal_vestiging, gewenst_merk, gewenst_soort_auto, offerte_LEADTRAJECT_EERSTEKWALIFICATIE, segment),
  deals_gemist.* except(deal_eind_datum, deal_afsluitreden, deal_vestiging, gewenst_merk),
  CASE
    WHEN email_stats.verstuurd_bericht <> "" AND email_stats.verstuurd_bericht LIKE "%Re-activatie%" 
      THEN "Re-activatie"
    WHEN email_stats.verstuurd_bericht <> "" AND email_stats.verstuurd_bericht NOT LIKE "%Re-activatie%" 
      THEN "Sales"
    ELSE NULL
  END AS type_flow,

FROM
(
  SELECT 
    *,
    binnen_90_dagen_lef_lead AS lead_na_reactivatie,
    binnen_90_dagen_sam_salestraject as salestraject_na_reactivatie,
    IF(binnen_90_dagen_sam_salestraject = 1 AND offerte_SALESTRAJECT_TRAJECTSTATUSID = "3", 1, 0) AS succesvol_salestraject_na_reactivatie,
    IF(binnen_90_dagen_sam_salestraject = 1 AND offerte_SALESTRAJECT_TRAJECTSTATUSID = "3", offerte_OFFERTE_TOTAALBEDRAG, 0) AS order_factuurbedrag_na_reactivatie,

  FROM
  (
    SELECT 
      *,
      ROW_NUMBER() OVER(PARTITION BY email ORDER BY binnen_90_dagen_sam_salestraject DESC, binnen_90_dagen_lef_lead DESC, offerte_SALESTRAJECT_TRAJECTSTATUSID DESC, offerte_SALESTRAJECT_CREATIEDATUM DESC, aangemaaktDatum DESC) as rank,

    FROM
    (
      SELECT 
        email_events.* except(extra_information, gewenst_merk, gewenst_model),
        lef_leads.* except(extra_information, gewenstMerk, gewenstModel),
        sam_salestrajecten.* except(extra_information, offerte_MERK_OMSCHRIJVING, offerte_AFLEVERINGMODEL_OMSCHRIJVING),
        IF(CAST(aangemaaktDatum AS DATE) > datum_bericht AND CAST(aangemaaktDatum AS DATE) <= DATE_ADD(datum_bericht, INTERVAL 90 DAY), 1, 0) AS binnen_90_dagen_lef_lead,
        IF(CAST(offerte_SALESTRAJECT_CREATIEDATUM AS DATE) > datum_bericht AND CAST(offerte_SALESTRAJECT_CREATIEDATUM AS DATE) <= DATE_ADD(datum_bericht, INTERVAL 90 DAY), 1, 0) AS binnen_90_dagen_sam_salestraject,
        ifnull(email_events.extra_information, IFNULL(lef_leads.extra_information, sam_salestrajecten.extra_information)) as extra_information,
        ifnull(email_events.gewenst_merk, IFNULL (lef_leads.gewenstMerk, sam_salestrajecten.offerte_MERK_OMSCHRIJVING)) as gewenst_merk,
        ifnull(email_events.gewenst_model, IFNULL (lef_leads.gewenstModel, sam_salestrajecten.offerte_AFLEVERINGMODEL_OMSCHRIJVING)) as gewenst_model
      FROM 
      (
        SELECT 
          datum_bericht,
          verstuurd_bericht,
          email,
          gewenst_merk,
          sales.* EXCEPT(datum_bericht, email, gewenst_merk, segment, verstuurd_bericht),
          segment,

        FROM ${ref("df_staging_views", "stg_hubspot_sales_emailstats")} sales
      ) email_events

      LEFT JOIN 
      (
        SELECT
          "LEF" as extra_information,
          pk_crm_id,
          LEFleadID,
          aangemaaktDatum,
          afgerondDatum,
          leadType,
          soortLead,
          kwalificatie,
          bron,
          systeem as lead_systeem,
          vestiging,
          resultaat,
          afsluitreden,
          gewenstMerk,
          gewenstModel,
          gewenstAutoSoort,
          email as contactwijzen
        
        FROM ${ref("df_rawdata_views", "lef_leads")}

        WHERE 
        REGEXP_CONTAINS(email, '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,5}')
      ) lef_leads
      ON 
        LOWER(TRIM(email_events.email)) = LOWER(TRIM(lef_leads.contactwijzen))

      LEFT JOIN 
      (
        SELECT 
          "SAM" as extra_information,
          offerte_SALESTRAJECT_DTCMEDIA_CRM_ID,
          offerte_SALESTRAJECT_TRAJECTID,
          offerte_SALESTRAJECT_CREATIEDATUM,
          offerte_SALESTRAJECT_AFGERONDDATUM,
          offerte_SALESTRAJECT_TRAJECTSTATUSID,
          offerte_SALESTRAJECT_SOORTAUTO,
          offerte_DEALER_NAAM,
          offerte_MERK_OMSCHRIJVING,
          offerte_AFLEVERINGMODEL_OMSCHRIJVING,
          offerte_OFFERTE_DATUMOFFERTE,
          offerte_OFFERTE_TOTAALBEDRAG,
          offerte_AUTOPRIJSCONSUMENT,
          offerte_TRAJECTAFSLUITREDEN_OMSCHRIJVING,
          offerte_LEADTRAJECT_EERSTEKWALIFICATIE,
          offerte_HERKOMST_OMSCHRIJVING,
          TRIM(IFNULL(offerte_RELATIE_EMAIL, RELATIE_EMAILZAKELIJK)) AS offerte_RELATIE_EMAIL

        FROM ${ref("df_staging_tables", "stg_sam_offertes")} 

        WHERE 
          REGEXP_CONTAINS(IFNULL(offerte_RELATIE_EMAIL, RELATIE_EMAILZAKELIJK), '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,5}')
      ) sam_salestrajecten
      ON 
        LOWER(TRIM(email_events.email)) = LOWER(TRIM(sam_salestrajecten.offerte_RELATIE_EMAIL))
    )
  )
  WHERE 
    rank = 1
) email_stats

FULL OUTER JOIN ${ref("stg_hubspot_deals_gemist_rendement")} deals_gemist
ON 1=0




`
let refs = pk.getRefs()
module.exports = {query, refs}
