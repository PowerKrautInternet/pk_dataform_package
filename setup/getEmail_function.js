let string = `
function getEmail(obj) {
 let contact = JSON.parse(obj).slice().filter((item, index) => { return item.contactwijzeType == "E-mail" }).pop();
 return ((contact) ? contact.value : null)
}`

module.exports = {string}