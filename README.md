# ReconWorks (Stage 1): Ingest → Staging

This repo is being built stage-by-stage.

## What’s implemented right now
✅ Stage 1: **Ingest raw CSV/XLSX inputs into SQLite staging tables** with traceability metadata.

It loads:
- `data/raw/transactions*.csv` → `stg_transactions_raw`
- `data/raw/vendor_payments*.csv` → `stg_vendor_payments_raw`

Each row gets:
- `batch_id` (UUID for the run)
- `ingested_at_utc`
- `source_file`
- `source_row_number`
- `row_hash` (sha256 of the original row content)

It also records a per-file registry in `ingest_files`.

## Quickstart

### 1) Install dependencies
```bash
pip install -r requirements.txt
```

### 2) Generate sample raw data
```bash
python -m reconworks init-sample-data
```

### 3) Run ingest
```bash
python -m reconworks ingest --config config.toml --export-csv
```

Outputs:
- SQLite DB: `out/sqlite/reconworks.db`
- CSV exports (latest batch): `out/csv/stg_*_raw.csv`

## Next stages (coming next in-chat)
- Stage 2: Mapping (canonical column alignment)
- Stage 3: Cleaning (parse dates/amounts)
- Stage 4: Normalization (vendor canonicalization)
- Stage 5+: QA, Matching, Exceptions, Reporting marts
