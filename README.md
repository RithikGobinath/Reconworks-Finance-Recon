# ReconWorks (Stages 1–4)

This repo is being built stage-by-stage as a portfolio project for Finance Ops automation.

## Stage 1: Ingest → Staging ✅
Loads raw CSV/XLSX exports into SQLite staging tables with traceability metadata:
- `stg_transactions_raw`
- `stg_vendor_payments_raw`
- `ingest_files`

## Stage 2: Mapping → Canonical columns ✅
Maps messy export columns into canonical fields:
- `vendor_raw`, `date_raw`, `amount_raw`
Outputs:
- `stg_transactions_mapped`
- `stg_vendor_payments_mapped`
- `mapping_runs`

## Stage 3: Cleaning → Typed fields ✅
Parses and cleans:
- `date_raw` → `date` (ISO)
- `amount_raw` → `amount_cents` (int)
Outputs:
- `clean_transactions`
- `clean_vendor_payments`
- `cleaning_runs`

## Stage 4: Normalization → Vendor canonicalization ✅
Normalizes vendor strings so matching and reporting work in the real world:
- `vendor_raw` → `vendor_clean` → `vendor_canonical`
Uses:
- `data/reference/vendor_aliases.csv` (regex patterns)
Outputs:
- `norm_transactions`
- `norm_vendor_payments`
- `normalization_runs`

## Quickstart

```bash
pip install -r requirements.txt
python -m pip install -e .
python -m reconworks init-sample-data

python -m reconworks ingest --config config.toml --export-csv
python -m reconworks map --config config.toml --export-csv
python -m reconworks clean --config config.toml --export-csv
python -m reconworks normalize --config config.toml --export-csv
```

Outputs:
- SQLite DB: `out/sqlite/reconworks.db`
- CSVs: `out/csv/*.csv`

## Stage 5: Modeling (dim/fact tables)
Builds:
- `dim_vendor` (unique canonical vendors)
- `fact_transactions`
- `fact_vendor_payments`

Run:
```bash
python -m reconworks model --config config.toml --export-csv
```

Outputs:
- SQLite: `dim_vendor`, `fact_transactions`, `fact_vendor_payments`
- CSV: `out/csv/fact_transactions.csv`, `out/csv/fact_vendor_payments.csv`
