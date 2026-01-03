# ReconWorks — Finance Ops Reconciliation & Reporting Toolkit

ReconWorks is a **stage-by-stage Python pipeline** that turns messy finance exports (CSV/XLSX) into:

- a traceable SQLite “single source of truth”
- **matched** transactions ↔ vendor payments
- an **exceptions queue** you can actually work from
- **pivot-friendly reporting marts**
- a ready-to-share **Excel dashboard**
- optional **Power Query “folder drops”** so Excel can refresh like a real Finance Ops workflow

This project is built to look and feel like how month-end work happens: source exports come in, data gets standardized, reconciliation happens, exceptions get reviewed, and leadership gets a dashboard.

---

## What problem this solves

If you’ve ever had to reconcile:
- card transactions (bank feed, p-card exports, etc.)
- vendor payments (ERP ledger exports, payment run reports, etc.)

…you know the pain:
- vendor names don’t match (“AMZN Mktp US*…“ vs “Amazon”)
- dates don’t line up perfectly (posting date vs ledger entry date)
- exports come from different systems with different columns
- you need an audit trail when someone asks “where did this number come from?”

ReconWorks automates that workflow end-to-end, but keeps things transparent (SQLite tables + CSV exports) so it’s easy to audit and easy to explain.

---

## Features

### Data pipeline + auditability
- Ingest **CSV or Excel** files as raw strings (no early type loss)
- Add traceability metadata (`batch_id`, `source_file`, `source_row_number`, `row_hash`)
- Preserve original column names in an ingest registry table (`ingest_files`)

### Finance ops “real world” cleanup
- Configurable column mapping (“Merchant” vs “Payee” vs “Vendor”)
- Robust amount parsing (currency symbols, commas, negatives like `(12.34)`)
- Vendor normalization with:
  - regex alias rules (`data/reference/vendor_aliases.csv`)
  - token cleanup (store numbers, extra codes, “LLC”, “INC”, etc.)
  - fallback behavior + confidence score

### Reconciliation engine
- Candidate generation with blocking on **date window** and **amount tolerance**
- Fuzzy vendor similarity using RapidFuzz `token_set_ratio`
- Weighted scoring (vendor/date/amount) + one-to-one matching
- Match typing (`exact`, `date_window`, `vendor_fuzzy`, `weak`)

### QA + exceptions workflow
- Built-in QA checks (missing fields, likely duplicates, weekend transactions, outliers)
- Optional policy rules file (simple CSV) to flag items for review
- Exceptions table that merges:
  - QA flags
  - unmatched transactions
  - unmatched vendor payments
  - low-confidence matches

### Reporting + Excel
- Reporting marts designed for PivotTables
- Excel dashboard build step (openpyxl) for a clean deliverable
- Optional Power Query publisher for “refresh from folder” workflows

---

## Repo layout

```
Reconworks-Finance-Recon/
  src/reconworks/                 # Python package (src-layout)
  data/
    raw/                          # drop your exports here (CSV/XLSX)
    reference/                    # vendor_aliases.csv, policy_rules.csv, …
  out/
    sqlite/reconworks.db          # single source of truth
    csv/                          # stage outputs (easy to inspect)
    excel/recon_dashboard.xlsx    # Excel dashboard
    pq_drop/                      # Power Query folder drops (optional)
  docs/
    powerquery_refresh.md         # how to wire Excel → PQ refresh
```

---

## Quickstart (2–3 minutes)

### 1) Set up Python environment

> Python 3.10+ is required.

Create a virtual environment (recommended) and install dependencies:

```bash
python -m venv .venv

# Windows (PowerShell)
.\.venv\Scripts\Activate.ps1

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
pip install -e .
```

### 2) Generate sample data

```bash
python -m reconworks init-sample-data
```

This writes:
- `data/raw/transactions_sample.csv`
- `data/raw/vendor_payments_sample.csv`
- `data/reference/vendor_aliases.csv` (basic regex aliases)

### 3) Run the pipeline end-to-end

```bash
python -m reconworks ingest     --config config.toml --export-csv
python -m reconworks map        --config config.toml --export-csv
python -m reconworks clean      --config config.toml --export-csv
python -m reconworks normalize  --config config.toml --export-csv
python -m reconworks model      --config config.toml --export-csv
python -m reconworks qa         --config config.toml --export-csv
python -m reconworks match      --config config.toml --export-csv
python -m reconworks exceptions --config config.toml --export-csv
python -m reconworks report     --config config.toml --export-csv
python -m reconworks build-excel --config config.toml
```

### 4) Open the outputs

- SQLite database: `out/sqlite/reconworks.db`
- CSV exports: `out/csv/`
- Excel dashboard: `out/excel/recon_dashboard.xlsx`

---

## Command reference (CLI)

Run this anytime:

```bash
python -m reconworks --help
```

Available stages:

- `init-sample-data` — generate sample raw + reference data
- `ingest` — Stage 1: ingest files → `stg_*_raw`
- `map` — Stage 2: map raw columns → canonical fields → `stg_*_mapped`
- `clean` — Stage 3: parse date + amount → `clean_*`
- `normalize` — Stage 4: vendor normalization → `norm_*`
- `model` — Stage 5: dim/fact model → `dim_vendor`, `fact_*`
- `qa` — Stage 6: QA flags → `qa_flags`
- `match` — Stage 7: reconciliation matching → `matches`, `match_candidates`
- `exceptions` — Stage 8: actionable review list → `exceptions`
- `report` — Stage 9: reporting marts → `rpt_*`
- `build-excel` — Stage 10: build Excel dashboard workbook
- `postmodel` — convenience runner for stages 6–9 (qa → match → exceptions → report)
- `publish-pq` — publish Power Query “folder drops” (optional refresh workflow)

> Most commands default to the **latest batch**. Many support `--batch-id` for deterministic re-runs.

---

## How batching works (important)

Each `ingest` run creates a new `batch_id` (UUID). That `batch_id` is carried through every downstream table.

Why this matters:
- you can run multiple batches (like multiple months) without overwriting the database
- downstream stages are designed to be **idempotent per batch** (re-running a stage replaces that batch’s output for the stage)

---

## Configuration (config.toml)

ReconWorks is controlled by a single config file: `config.toml`.

### Project + output paths

```toml
[project]
name = "ReconWorks"
output_dir = "out"
database_path = "out/sqlite/reconworks.db"
```

### Sources (what files to ingest)

Each source defines a file glob and (optional) mapping hints:

```toml
[sources.transactions]
path = "data/raw/transactions*.csv"

[sources.transactions.mapping]
vendor_raw = ["merchant", "vendor", "payee"]
date_raw   = ["post_date", "transaction_date", "date"]
amount_raw = ["amount", "amt"]
```

Same idea for `vendor_payments`.

### Reference files

```toml
[reference]
vendor_aliases_path = "data/reference/vendor_aliases.csv"
policy_rules_path   = "data/reference/policy_rules.csv"
```

### Matching thresholds

```toml
[matching]
date_window_days = 3
amount_tolerance_cents = 0
min_score = 0.85
low_confidence_threshold = 0.90

vendor_weight = 0.6
date_weight = 0.3
amount_weight = 0.1
```

### Power Query publisher (optional)

```toml
[powerquery]
drop_root = "out/pq_drop"
mode = "history"     # "history" or "latest"
```

- `history` writes timestamped snapshots under `out/pq_drop/history/<dataset>/...csv`
- `latest` overwrites stable filenames in `out/pq_drop/latest/*.csv`

---

## Stage-by-stage: what happens to the data

This is the “finance ops story” of the pipeline:

### Stage 1 — Ingest → SQLite staging
**Input:** raw exports (CSV/XLSX)  
**Output tables:** `stg_transactions_raw`, `stg_vendor_payments_raw`, `ingest_files`

What changes:
- columns are sanitized (spaces → underscores, etc.) to be SQLite-safe
- metadata columns are added (batch/source/row)
- a stable `row_hash` is computed to enable dedupe + traceability

### Stage 2 — Mapping → Canonical fields
**Output tables:** `stg_*_mapped`, `mapping_runs`

What changes:
- pulls values into canonical fields:
  - `vendor_raw`
  - `date_raw`
  - `amount_raw`
- records which source columns were used (`map_vendor_from`, etc.)
- marks rows with `mapping_status = error` if required fields can’t be found

### Stage 3 — Cleaning → Typed fields
**Output tables:** `clean_*`, `cleaning_runs`

What changes:
- parses:
  - `date_raw` → `date` (ISO `YYYY-MM-DD`)
  - `amount_raw` → `amount_cents` (integer)
- sets `clean_status` and `clean_notes` for troubleshooting

### Stage 4 — Normalization → Canonical vendor
**Output tables:** `norm_*`, `normalization_runs`

What changes:
- `vendor_raw` → `vendor_clean` (noise removed)
- `vendor_clean` + regex rules → `vendor_canonical`
- produces method + confidence:
  - `alias_regex` (high confidence)
  - `clean_fallback` (medium confidence)

### Stage 5 — Modeling → Dim/Fact
**Output tables:** `dim_vendor`, `fact_transactions`, `fact_vendor_payments`, `modeling_runs`

What changes:
- builds `vendor_id` dimension (hash of canonical vendor)
- creates fact IDs (`txn_id`, `pay_id`)
- derives fields used in reporting:
  - `month` (`YYYY-MM`)
  - `year`
  - `is_weekend`

### Stage 6 — QA checks → Flags
**Output tables:** `qa_flags`, `qa_runs`

Includes:
- missing vendor/date/amount checks
- likely duplicates (same vendor/date/amount)
- weekend transactions
- amount outliers
- optional policy rules (see below)

### Stage 7 — Matching → Reconciliation
**Output tables:** `match_candidates`, `matches`, `matching_runs`

Matching logic (high level):
1. **Block** to reduce candidates:
   - only consider items within `date_window_days`
   - only consider items within `amount_tolerance_cents`
2. Compute vendor similarity using RapidFuzz `token_set_ratio`
3. Compute a weighted score:
   - vendor similarity (0–1)
   - date similarity (0–1)
   - amount similarity (0–1)
4. Greedy one-to-one selection:
   - highest score wins
   - each txn and each payment can only match once

### Stage 8 — Exceptions → Action queue
**Output tables:** `exceptions`, `exception_runs`

Exceptions are created from:
- QA flags
- unmatched transactions
- unmatched vendor payments
- low-confidence matches (`match_score < low_confidence_threshold`)

This becomes your “worklist”.

### Stage 9 — Reporting marts
**Output tables:** `rpt_*`, `report_runs`

Designed for pivots:
- `rpt_spend_by_month_vendor`
- `rpt_match_rate_by_month`
- `rpt_exceptions_by_code`
- `rpt_top_vendors`

### Stage 10 — Excel dashboard
**Output:** `out/excel/recon_dashboard.xlsx`

The workbook includes:
- summary KPIs
- exceptions sheet (review list)
- match stats + spend summaries
- simple charts

---

## Reference data files you can edit

### vendor_aliases.csv (vendor normalization)

Path: `data/reference/vendor_aliases.csv`

Format:

```csv
pattern,canonical_vendor
AMZN|AMAZON,Amazon
UBER,Uber
STARBUCKS,Starbucks
```

- `pattern` is a case-insensitive regex
- the first match wins

### policy_rules.csv (QA policy flags)

Path: `data/reference/policy_rules.csv`

Format:

```csv
flag_code,field,op,value,severity,message,applies_to
POLICY_REVIEW_OVER_20,amount_cents,>,2000,info,Review transactions over $20,transactions
```

---

## Power Query refresh workflow (optional but very “ops”)

There are two good options:

### Option A — Simple: load the current CSV outputs
Point Power Query directly at `out/csv/*.csv`. This is simplest if you don’t need run history.

### Option B — Folder drops: append snapshots (recommended)
Publish timestamped drops:

```bash
python -m reconworks publish-pq --config config.toml
```

Then in Excel:
- Data → Get Data → From File → From Folder
- Choose `out/pq_drop/history/<dataset>`
- Combine & Transform
- Data → Refresh All

For full walkthrough, see: `docs/powerquery_refresh.md`.

---

## Testing

Run the unit tests:

```bash
pytest -q
```

---

## Notes / limitations (honest + helpful)

- This is intentionally built for “finance ops scale” (thousands to low millions of rows).  
  If you need huge volumes, the exact same stages can be migrated to a warehouse.
- Matching is currently one-to-one. Partial payments or many-to-one invoice splits are a logical next upgrade.

---

## References (useful docs)

Semantic Versioning:
- https://semver.org/

Keep a Changelog:
- https://keepachangelog.com/

Git tags:
- https://git-scm.com/book/en/v2/Git-Basics-Tagging

GitHub Releases:
- https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository

Power Query: combine files from a folder:
- https://support.microsoft.com/en-us/office/import-data-from-a-folder-with-multiple-files-power-query-94b8023c-2e66-4f6b-8c78-6a00041c90e4

RapidFuzz token_set_ratio:
- https://rapidfuzz.github.io/RapidFuzz/Usage/fuzz.html
