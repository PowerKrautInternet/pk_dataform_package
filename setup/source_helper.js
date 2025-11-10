
class SourceHelper {
  constructor(defaultDatabase, defaultSchema = 'rawdata') {
    this.defaultDatabase = defaultDatabase;
    this.defaultSchema = defaultSchema;
    this.sources = [];
  }

  addSource(name, table, schema = this.defaultSchema, database = this.defaultDatabase) {
    const source = { name, table, schema, database };
    this.sources.push(source);
    return source;
  }

  getSource(name) {
    return this.sources.find(src => src.name === name);
  }

  listSources() {
    return this.sources;
  }
}

module.exports = SourceHelper;
