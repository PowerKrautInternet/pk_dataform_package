class FunctionObject {
    constructor(config) {
        this.database = config.database ?? dataform.projectConfig.defaultDatabase;
        this.schema = config.schema ?? "rawdata";
        this.name = config.name ?? null;
        this.function = config.function ?? null;
        this.vars = config.vars ?? {}
        this.return_type = config.return_type ?? "STRING"
        this.type = config.function_type ?? "javascript"
        this.sql = this;
    }

    set function_type(function_type) {
        this.type = function_type.toLowerCase();
    }

    get language() {
        switch (this.type) {
            case "javascript":
                return ` LANGUAGE js AS R""" `;
            case "sql":
                return " AS ( ";
            default:
                throw new Error(`Switch error! setup/language/${this.name}`);
        }
    }

    get return() {
        switch (this.type) {
            case "javascript":
                return ` return ${this.sql_object.name}(${this.sql_object.params});
                        """`
            case "sql":
                return ` ); `
            default:
                throw new Error(`Switch error! setup/return/${this.name}`);
        }
    }
    
    get sql() {
        if(this.function && this.name){
            return this.sqlForFunction
        }else{
            return `//NO VALID CONFIG FOR ${this.name}`
        }
    }

    set sql(the_function_object) {
        this.sql_object = the_function_object;
        this.sqlForFunction = `CREATE OR REPLACE FUNCTION 
        \`${the_function_object.database}.${the_function_object.schema}.${the_function_object.name}\` (${the_function_object.vars}) 
        RETURNS ${the_function_object.return_type} 
        ${this.language}
        ${the_function_object.function}
        ${this.return}`

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
