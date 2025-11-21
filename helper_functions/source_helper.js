

class SourceHelper {
  constructor(defaultDatabase = null, defaultSchema = 'rawdata') {
      if(defaultDatabase == null){
          throw new Error("No source_helper/DefaultDatabase")
      }
    this.default_database = defaultDatabase;
    this.default_schema = defaultSchema;
    this.sources = [];
  }

  ref(ref_source) {
      //validating type of params
      if(typeof ref_source !== "object"){
          throw new Error("source_helper/ref/ref_source is not an valid type")
      }

      //validating req fields
      if(!ref_source.name){
          throw new Error("source_helper/ref/ref_source does not have the required fields")
      }

      //getting the ref source from sources
      let source = this.getSource(ref_source)

      //check if there is an ref found
      if(!source){
          throw new Error("source_helper/ref/source is invalid")
      }

      //returning ref
      return `\`${source.database}.${source.schema}.${source.name}\``
  }

  addSource(name, schema = this.defaultSchema, database = this.defaultDatabase) {
    const source = { name, schema, database };
    this.sources.push(source);
    return source;
  }

  getSource(get_source) {
    return this.sources.find(src => src.name === get_source.name);
  }

  setSources(varSource) {
    this.sources = [];

    for (const s in varSource) {
      const v = varSource[s];

      // Verwerkt de 'publishers'-array
      v.publishers = (v.publishers ?? [])
        .filter(publisher => !!publisher.name)
        .flatMap(publisher => {
          const names = Array.isArray(publisher.name)
            ? publisher.name
            : [publisher.name];

          return names.map(name => ({
            name,
            recency: publisher.recency ?? true,
            freshnessDays: publisher.freshnessDays ?? v.freshnessDays ?? 1,
          }));
        });

        // Interne flags
        v.database = v.database ?? this.default_database
        v.noSuffix = true;
        v.declaredSource = true;

      this.sources.push(v);
    }
  }

  listSources() {
    return this.sources;
  }
}

module.exports = SourceHelper;

