// Last edit: Stijn Holtus
// 2-6-2025
// queryBuilder
class Query {
    select;
    from;
    where;
    group_by;

    constructor(baseQuery) {
        baseQuery = this.parseSelect(baseQuery);
        baseQuery = this.parseFrom(baseQuery);
        baseQuery = this.parseWhere(baseQuery);
        this.parseGroupBy(baseQuery);
    }

    parseSelect(baseQuery) {
        return baseQuery;
    }

    parseFrom(baseQuery) {
        return baseQuery;
    }

    parseWhere(baseQuery) {
        return baseQuery;
    }

    parseGroupBy(baseQuery) {
        return baseQuery;
    }
}

module.exports = {Query};