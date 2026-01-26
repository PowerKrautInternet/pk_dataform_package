function googleSheetsExternalTable({
  tableName,
  sheetUrl,
  sheetRange,
  columns,
  project,   
  schema    
}) {
  if (!tableName) throw new Error("googleSheetsExternalTable: tableName is required");
  if (!sheetUrl) throw new Error("googleSheetsExternalTable: sheetUrl is required");
  if (!sheetRange) throw new Error("googleSheetsExternalTable: sheetRange is required");
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new Error("googleSheetsExternalTable: columns must be a non-empty array");
  }

  const columnSql = columns
    .map(c => {
      if (!c?.name || !c?.type) {
        throw new Error("googleSheetsExternalTable: each column needs { name, type }");
      }
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(c.name)) {
        throw new Error(`googleSheetsExternalTable: invalid column name: ${c.name}`);
      }
      return `${c.name} ${c.type}`;
    })
    .join(",\n      ");

  return operate(tableName, {
    schema: schema || "googleSheets",
    database: project,
    tags: ["external", "google_sheets"]
  }).queries(ctx => {
    const resolvedProject = project || ctx.projectConfig.defaultDatabase;
    const resolvedSchema = schema || "googleSheets";

    return `
      CREATE OR REPLACE EXTERNAL TABLE
        \`${resolvedProject}.${resolvedSchema}.${tableName}\`
      (
        ${columnSql}
      )
      OPTIONS (
        format = 'GOOGLE_SHEETS',
        uris = ['${sheetUrl}'],
        skip_leading_rows = 1,
        sheet_range = '${sheetRange}'
      )
    `;
  });
}

module.exports = { googleSheetsExternalTable };
