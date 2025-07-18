/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `

    SELECT count(distinct json_value(payload, '$.email')), current_date() as receivedon FROM ${ref("rawdata","hubspotExporterDataProducer")} where json_value(payload, '$.type') = 'HubspotContactsExporterPublisher'

`
let refs = pk.getRefs()
module.exports = {query, refs}
