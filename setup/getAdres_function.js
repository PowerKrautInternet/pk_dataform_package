let string = `
function getAdres(obj, field_name) {
 let response = JSON.parse(obj);
 return ((response && response.slice().length > 0) ? response.slice().pop()[field_name] : null)
}`

module.exports = {string}