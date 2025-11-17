class FunctionObject {
    constructor(config) {
        this.database = config.database ?? dataform.projectConfig.defaultDatabase;
        this.schema = config.schema ?? "rawdata";
        this.name = config.name ?? null;
        this.function = config.function ?? null;
        this.vars = config.vars ?? {}
        this.return_type = config.return_type ?? "STRING"
        this.sql = this;
    }

    get sql() {
        if(this.function && this.name){
            return this.sqlForFunction
        }else{
            return `//NO VALID CONFIG FOR ${this.name}`
        }
    }

    set sql(the_function_object) {
        this.sqlForFunction = `CREATE OR REPLACE FUNCTION 
        \`${the_function_object.database}.${the_function_object.schema}.${the_function_object.name}\` (${the_function_object.vars}) 
        RETURNS ${the_function_object.return_type} LANGUAGE js AS R""" 
            ${the_function_object.function}
            return ${the_function_object.name}(${the_function_object.params});
        """`
    }

    // Helper method to format the parameters for the function signature
    get vars() {
        return Object.entries(this.varsForFunction)
            .map(([param, type]) => `${param} ${type}`)
            .join(", ");
    }
    set vars(parameterVars) {
        this.varsForFunction = parameterVars
    }
    // Helper method to format the actual parameters in the return statement
    get params() {
        return Object.keys(this.varsForFunction).join(", ");
    }
    get source(){
        return {
            config: {
                database: this.database,
                schema: this.schema
            },
            type: "function",
            name: this.name
        }
    }
}
module.exports = FunctionObject
