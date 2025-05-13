/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT
        json_value(PAYLOAD, '$.dtcmedia_crm_id') AS dtcmedia_crm_id,
        json_value(PAYLOAD, '$.type') AS type,
        json_value(PAYLOAD, '$.id') AS order_id,
        json_value(PAYLOAD, '$.response.vsb') AS vsb,
        json_value(PAYLOAD, '$.response.a_car_id') AS a_car_id,
        json_value(PAYLOAD, '$.response.licenseplate') AS licenseplate,
        json_value(PAYLOAD, '$.response.order_status') AS order_status,
        CAST(json_value(PAYLOAD, '$.response.date_delivery') AS DATE) AS date_delivery,
        json_value(PAYLOAD, '$.response.time_delivery') AS time_delivery,
        json_value(PAYLOAD, '$.response.is_car_connect') AS is_car_connect,
        json_value(PAYLOAD, '$.response.driver_name') AS driver_name,
        json_value(PAYLOAD, '$.response.driver_mail') AS driver_mail,
        json_value(PAYLOAD, '$.response.customergroup') AS customergroup,
        json_value(PAYLOAD, '$.response.establishment') AS establishment,
        json_value(PAYLOAD, '$.response.brand') AS brand,
        json_value(PAYLOAD, '$.response.model') AS model,
        json_value(PAYLOAD, '$.response.salesperson') AS salesperson

    FROM ${ref("df_rawdata_views", "csvDataProducer_lasttransaction")}
    WHERE
        json_value(PAYLOAD, '$.type') = "csvSyntecDumpApAlleOrdersPublisher"
      AND json_value(PAYLOAD, '$.dtcmedia_crm_id') = "982"
    
`
let refs = pk.getRefs()
module.exports = {query, refs}