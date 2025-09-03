let string = `
function getTableName(obj) {
    let contact = Object.keys(JSON.parse(obj))[0];
    return contact.replace(/ID$/ig, '');
}
return getTableName(json_row);`

module.exports = {string}
