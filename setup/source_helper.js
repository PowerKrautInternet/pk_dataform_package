class SourceHelper {
  constructor(defaultDatabase, defaultSchema = 'rawdata') {
    this.defaultDatabase = defaultDatabase;
    this.defaultSchema = defaultSchema;
    this.sources = [];
  }

  addSource(name, schema = this.defaultSchema, database = this.defaultDatabase) {
    const source = { name, schema, database };
    this.sources.push(source);
    return source;
  }

  getSource(name) {
    return this.sources.find(src => src.name === name);
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

