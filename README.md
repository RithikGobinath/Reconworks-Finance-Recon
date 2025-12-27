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


## Stage 6: QA checks

Run:
```bash
python -m reconworks qa --config config.toml --export-csv
```

Outputs:
- SQLite: `qa_flags`, `qa_runs`
- CSV: `out/csv/qa_flags.csv`

Optional: add `data/reference/policy_rules.csv` to define custom flags.
Example:
```csv
flag_code,field,op,value,severity,message,applies_to
POLICY_REVIEW_OVER_20,amount_cents,>,2000,info,Review transactions over $20,transactions
```


## Stage 7: Matching (reconciliation engine)

Run:
```bash
python -m reconworks match --config config.toml --export-csv
```

Outputs:
- SQLite: `match_candidates`, `matches`, `matching_runs`
- CSV: `out/csv/match_candidates.csv`, `out/csv/matches.csv`, `out/csv/unmatched_transactions.csv`, `out/csv/unmatched_vendor_payments.csv`

Matching uses fuzzy vendor similarity via RapidFuzz token set ratio (robust to extra tokens). 

Tune thresholds in `config.toml` under `[matching]`.

## Stage 8: Exceptions (actionable review list)

Run:
```bash
python -m reconworks exceptions --config config.toml --export-csv
```

Outputs:
- SQLite: `exceptions`, `exception_runs`
- CSV: `out/csv/exceptions.csv`

Exceptions include QA flags + unmatched items + low-confidence matches.

## Stage 9: Reporting marts (pivot-friendly)

Run:
```bash
python -m reconworks report --config config.toml --export-csv
```

Outputs:
- SQLite: `rpt_spend_by_month_vendor`, `rpt_match_rate_by_month`, `rpt_exceptions_by_code`, `rpt_top_vendors`
- CSV exports in `out/csv/`

## Stage 10: Excel dashboard

Run:
```bash
python -m reconworks build-excel --config config.toml
```

Creates `out/excel/recon_dashboard.xlsx` with:
- Summary KPIs
- Exceptions, Matches, QA flags
- Spend + match-rate sheets
- Simple charts (openpyxl). 

Optional (Excel users): you can also connect Excel to `out/csv/` using Power Query “From Folder” and refresh to pull the latest CSV outputs. 

## Convenience: run stages 6-9

```bash
python -m reconworks postmodel --config config.toml --export-csv
```


## Optional: Power Query refresh workflow

If you want an ops-style Excel that refreshes from folder drops, see `docs/powerquery_refresh.md`.

Quick start:
```bash
python -m reconworks publish-pq --config config.toml
```
Then in Excel: Data → Get Data → From Folder → Combine & Transform.
