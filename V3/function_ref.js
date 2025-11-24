const pkg = {
    handler: null,

    setHandler(fn) {
        this.handler = fn;
    },

    run() {
        if (typeof this.handler !== "function") {
            throw new Error("No handler set");
        }
        return this.handler("some_table",{ type: "table", schema: "test_tables"}).query("SELECT * FROM `test`");
    }
};

module.exports = pkg;
