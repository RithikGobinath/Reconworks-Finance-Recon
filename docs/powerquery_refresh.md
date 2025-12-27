# Power Query refresh workflow (Ops-style)

This project can export clean CSVs to `out/csv/`. To make a **refreshable** Excel dashboard that updates with new runs, use Power Query.

## Option A: Simple (one CSV per dataset)

Use this when your pipeline overwrites the same filenames in `out/csv/` (default).

1. In Excel: **Data → Get Data → From File → From Text/CSV**
2. Pick one file, e.g. `out/csv/exceptions.csv`
3. Choose **Transform Data** to open Power Query
4. In Power Query: set types, rename columns if needed
5. **Home → Close & Load To…**
   - Choose **Only Create Connection**
   - Optionally check **Add this data to the Data Model**
6. Repeat for the other outputs you care about:
   - `matches.csv`, `unmatched_transactions.csv`, `unmatched_vendor_payments.csv`
   - reporting marts: `rpt_spend_by_month_vendor.csv`, etc.
7. Back in Excel: **Data → Refresh All** whenever you re-run ReconWorks.

Microsoft’s walkthrough for combining files from a folder is here (Option B) and the Refresh All behavior is documented here:
- Import from folder (combine files): https://support.microsoft.com/en-us/office/import-data-from-a-folder-with-multiple-files-power-query-94b8023c-2e66-4f6b-8c78-6a00041c90e4
- Refresh your query: https://support.microsoft.com/en-us/office/add-data-and-then-refresh-your-query-44e3e65a-59d4-4ea6-b681-2e48e983d7b8

## Option B: “Real ops team” (folder drops + combine files)

This is closer to how teams run month-end: each run drops a new file into a folder and Excel appends them automatically.

### 1) Publish versioned drops

Run:
```bash
python -m reconworks publish-pq --config config.toml
```

This writes versioned snapshots under:
- `out/pq_drop/history/<dataset>/...csv`

You can switch to stable filenames by setting in `config.toml`:
```toml
[powerquery]
mode = "latest"
```
and re-running `publish-pq`. That writes into `out/pq_drop/latest/`.

### 2) Create a Power Query that combines files

For a dataset folder (example: exceptions), in Excel:
1. **Data → Get Data → From File → From Folder**
2. Select: `out/pq_drop/history/exceptions`
3. Click **Combine & Transform**
4. Excel will auto-generate a function and append all files

### 3) Turn on auto-refresh (optional)

In Excel:
- **Data → Queries & Connections**
- Right-click a query → **Properties**
- Enable **Refresh data when opening the file**
- Optional: **Refresh every N minutes**

## Notes

- For large folders, prefer **history** only for key datasets (exceptions, matches) and keep the rest on **latest**.
- Keep schemas stable (don’t rename CSV columns casually) so Power Query refreshes smoothly.
