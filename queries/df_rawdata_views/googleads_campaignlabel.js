/*config*/
let pk = require("../../sources")
let ref = pk.ref
let query = `
SELECT
campaign_id,
MAX(campaign_name) AS campaign_name,
MAX(IF(STARTS_WITH(label_name, 'Merk:'), TRIM(SUBSTR(label_name, LENGTH('Merk:') + 1)), NULL)) AS merk,
MAX(IF(STARTS_WITH(label_name, 'Campagnegroep:'), TRIM(SUBSTR(label_name, LENGTH('Campagnegroep:') + 1)), NULL)) AS campagnegroep,
MAX(IF(STARTS_WITH(label_name, 'Model:'), TRIM(SUBSTR(label_name, LENGTH('Model:') + 1)), NULL)) AS model
FROM
${ref("ads_CampaignLabel")}
WHERE _LATEST_DATE = _DATA_DATE
GROUP BY campaign_id
`
let refs = pk.getRefs()
module.exports = {query, refs}
