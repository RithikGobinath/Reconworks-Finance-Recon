/*
fnLoadCsv: helper function to load a CSV file with consistent settings.
Usage:
  fnLoadCsv(File.Contents(path))
Docs: Csv.Document https://learn.microsoft.com/en-us/powerquery-m/csv-document
*/
let
  fnLoadCsv = (content as binary) as table =>
    let
      Source = Csv.Document(content, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
      Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true])
    in
      Promoted
in
  fnLoadCsv
