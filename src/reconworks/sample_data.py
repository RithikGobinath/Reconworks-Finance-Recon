from __future__ import annotations

from pathlib import Path
import pandas as pd
from .util import ensure_dir

def write_sample_raw(repo_root: Path) -> None:
    raw_dir = repo_root / "data" / "raw"
    ref_dir = repo_root / "data" / "reference"
    ensure_dir(raw_dir)
    ensure_dir(ref_dir)

    tx = pd.DataFrame([
        {"Transaction ID": "T1001", "Post Date": "2025-12-02", "Transaction Date": "2025-12-01", "Merchant": "AMZN Mktp US*2H3K21", "Amount": "48.27", "Currency": "USD", "Card Last4": "1234", "Memo": "Laptop stand"},
        {"Transaction ID": "T1002", "Post Date": "2025-12-03", "Transaction Date": "2025-12-03", "Merchant": "UBER TRIP HELP.UBER.COM", "Amount": "17.90", "Currency": "USD", "Card Last4": "1234", "Memo": "Client visit"},
        {"Transaction ID": "T1003", "Post Date": "2025-12-05", "Transaction Date": "2025-12-05", "Merchant": "STARBUCKS #04921 MADISON", "Amount": "6.45", "Currency": "USD", "Card Last4": "5678", "Memo": ""},
    ])
    tx.to_csv(raw_dir / "transactions_sample.csv", index=False)

    vp = pd.DataFrame([
        {"Entry ID": "L9001", "Entry Date": "2025-12-02", "Payee": "Amazon", "Amount": "48.27", "Category": "Office Supplies", "Account": "6100", "Cost Center": "IT", "Reference": "", "Description": "Amazon purchase"},
        {"Entry ID": "L9002", "Entry Date": "2025-12-03", "Payee": "Uber", "Amount": "17.90", "Category": "Travel", "Account": "6200", "Cost Center": "Sales", "Reference": "", "Description": "Ride to client"},
        {"Entry ID": "L9003", "Entry Date": "2025-12-05", "Payee": "Starbucks", "Amount": "6.45", "Category": "Meals", "Account": "6300", "Cost Center": "Sales", "Reference": "", "Description": "Coffee"},
    ])
    vp.to_csv(raw_dir / "vendor_payments_sample.csv", index=False)

    vendor_aliases = pd.DataFrame([
        {"pattern": "AMZN|AMAZON", "canonical_vendor": "Amazon"},
        {"pattern": "UBER", "canonical_vendor": "Uber"},
        {"pattern": "STARBUCKS", "canonical_vendor": "Starbucks"},
    ])
    vendor_aliases.to_csv(ref_dir / "vendor_aliases.csv", index=False)
