/*
Example query: combine all exceptions snapshots from the history folder.

Set FolderPath to your local repo path, then paste into Power Query Advanced Editor.

Docs: Folder.Files + Csv.Document
- Import from folder (combine files): https://support.microsoft.com/en-us/office/import-data-from-a-folder-with-multiple-files-power-query-94b8023c-2e66-4f6b-8c78-6a00041c90e4
- Csv.Document: https://learn.microsoft.com/en-us/powerquery-m/csv-document
*/
let
  FolderPath = "C:\\path\\to\\Reconworks-Finance-Recon\\out\\pq_drop\\history\\exceptions",
  Source = Folder.Files(FolderPath),
  KeepCsv = Table.SelectRows(Source, each Text.EndsWith([Extension], ".csv")),
  AddedTable = Table.AddColumn(KeepCsv, "Data", each Csv.Document([Content], [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv])),
  Expanded = Table.ExpandTableColumn(AddedTable, "Data", Table.ColumnNames(AddedTable{0}[Data])),
  Promoted = Table.PromoteHeaders(Expanded, [PromoteAllScalars=true])
in
  Promoted
