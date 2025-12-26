# ReconWorks (Stage 2): Mapping → Canonical Columns

This repo is being built stage-by-stage.

## What’s implemented right now
✅ Stage 1: **Ingest** raw CSV/XLSX inputs into SQLite staging tables with traceability metadata.  
✅ Stage 2: **Mapping** aligns messy input columns into canonical fields used by later stages:
- `vendor_raw`
- `date_raw`
- `amount_raw`

Stage 2 creates:
- `stg_transactions_mapped`
- `stg_vendor_payments_mapped`

It records mapping decisions in `mapping_runs`.

## Quickstart

### 1) Install dependencies
```bash
pip install -r requirements.txt
```

### 2) Generate sample raw data
```bash
python -m reconworks init-sample-data
```

### 3) Run ingest (Stage 1)
```bash
python -m reconworks ingest --config config.toml --export-csv
```

### 4) Run mapping (Stage 2)
```bash
python -m reconworks map --config config.toml --export-csv
```

Outputs:
- SQLite DB: `out/sqlite/reconworks.db`
- CSV exports (latest batch): `out/csv/stg_*_mapped.csv`

## Next stages (coming next in-chat)
- Stage 3: Cleaning (parse dates/amounts into typed columns)
- Stage 4: Normalization (vendor canonicalization)
- Stage 5+: QA, Matching, Exceptions, Reporting marts
