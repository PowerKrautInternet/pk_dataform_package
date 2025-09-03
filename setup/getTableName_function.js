let string = `
function getTableName(obj) {
    let contact = Object.keys(JSON.parse(obj))[0];
    return contact.replace(/ID$/ig, '');
}`

module.exports = {string}
