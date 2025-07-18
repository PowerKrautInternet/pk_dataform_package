/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
    

SELECT 
    IFNULL(leads.bron, orders.bron) as bron,
    IFNULL(CAST(leads.aangelegd AS DATE), orders.date_delivery) as record_date,
    lead_id as syntec_lead_id,
    kanaal,
    kanaal_groep as syntec_kanaal_groep,
    ordersoort as syntec_ordersoort,
    onderwerp,
    IFNULL(vestiging, establishment) AS vestiging,
    IFNULL(leads.merk, orders.brand) as merk,
    IFNULL(leads.MODEL,orders.model) AS model,
    IFNULL(leads.kenteken,orders.licenseplate) AS kenteken,
    CAST(aangelegd AS DATE) AS syntec_aangelegd,
    CAST(datum_gesloten AS DATE) AS syntec_datum_gesloten,
    sluitreden as syntec_sluitreden,
    leads.order_id as syntec_lead_order_id,
    klantkoppeling as syntec_klantkoppeling,
    IFNULL(gesloten_door,salesperson) AS syntec_verkoper,
    laatste_opmerking as syntec_laatste_opmerking,
    contactmomenten as syntec_contactmomenten,
    min_voor_geopend as syntec_min_voor_geopend,
    orders.order_id as syntec_order_id,
    orders.order_status as syntec_order_status,
    orders.date_delivery as syntec_date_delivery,
    orders.customergroup as syntec_customergroup

FROM (SELECT 'Syntec leads' as bron, * FROM ${ref("df_rawdata_views", "syntec_leads")}) leads

FULL OUTER JOIN (SELECT 'Syntec orders' as bron, * FROM ${ref("df_rawdata_views", "syntec_orders")}) orders
ON 1=0

WHERE IFNULL(CAST(leads.aangelegd AS DATE), orders.date_delivery) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)

    
    `
let refs = pk.getRefs()
module.exports = {query, refs}