// Last edit: Stijn Holtus
// 2-6-2025
// queryBuilder
class Query {
    select;
    from;
    where;
    group_by;

    constructor(baseQuery) {
        baseQuery = baseQuery.toLowerCase();
        baseQuery = this.parseSelect(baseQuery);
        baseQuery = this.parseFrom(baseQuery);
        baseQuery = this.parseWhere(baseQuery);
        this.parseGroupBy(baseQuery);
    }

    parseSelect(baseQuery) {
        //remove select and start there (ignore everything before)
        baseQuery = baseQuery.split("select") //select [*, source.something as something_else, FROM....
        baseQuery[0] = baseQuery[0].split(","); //[*] [source.something as something_else] [FROM...
        for (let i in baseQuery[0]) { // [i=0 [*], i=1 [source.something as something_else]] [FROM....
            baseQuery[0][i] = baseQuery[0][i].split("as");
            let alias = baseQuery[0][1]

        }

        return baseQuery;
    }

    parseFrom(baseQuery) {
        this.from = parse("FROM", baseQuery);
        return baseQuery;
    }

    parseWhere(baseQuery) {
        this.where = parse("WHERE", baseQuery);
        return baseQuery;
    }

    parseGroupBy(baseQuery) {
        this.groupBy = parse("GROUP BY", baseQuery);
        return baseQuery;
    }
}

module.exports = {Query};