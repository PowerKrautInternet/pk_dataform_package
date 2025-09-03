let string = `

function getTelefoon(obj) {
 let contact = JSON.parse(obj).slice().filter((item, index) => { return item.contactwijzeType == "Telefoon" }).pop();
 return ((contact) ? contact.value : null)
}

`
module.exports = string
