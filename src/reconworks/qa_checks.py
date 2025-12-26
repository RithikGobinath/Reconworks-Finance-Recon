from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from .util import utc_now_iso

@dataclass(frozen=True)
class PolicyRule:
    flag_code: str
    field: str
    op: str
    value: str
    severity: str
    message: str
    applies_to: str  # "transactions", "vendor_payments", or "both"

def load_policy_rules(path: Path) -> List[PolicyRule]:
    if not path.exists():
        return []
    rules: List[PolicyRule] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("flag_code"):
                continue
            rules.append(PolicyRule(
                flag_code=row["flag_code"].strip(),
                field=(row.get("field") or "").strip(),
                op=(row.get("op") or "").strip(),
                value=str(row.get("value") or "").strip(),
                severity=(row.get("severity") or "warning").strip(),
                message=(row.get("message") or "").strip(),
                applies_to=(row.get("applies_to") or "both").strip(),
            ))
    return rules

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _compare(series: pd.Series, op: str, value: str) -> pd.Series:
    # Numeric compare if possible
    s_num = _to_num(series)
    v_num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if not np.isnan(v_num) and s_num.notna().any():
        if op == ">": return s_num > v_num
        if op == ">=": return s_num >= v_num
        if op == "<": return s_num < v_num
        if op == "<=": return s_num <= v_num
        if op == "==": return s_num == v_num
        if op == "!=": return s_num != v_num
    # Fallback string compare
    s = series.astype(str)
    v = str(value)
    if op == "==": return s == v
    if op == "!=": return s != v
    return pd.Series([False] * len(series), index=series.index)

def run_qa_for_batch(
    batch_id: str,
    fact_transactions: pd.DataFrame,
    fact_vendor_payments: pd.DataFrame,
    policy_rules: List[PolicyRule],
) -> pd.DataFrame:
    """Generate QA flags (no DB I/O)."""
    created_at = utc_now_iso()
    flags: List[Dict[str, Any]] = []

    def add_flags(df: pd.DataFrame, record_type: str, id_col: str, cond: pd.Series, code: str, severity: str, message: str):
        if df.empty:
            return
        hit = df[cond.fillna(False)].copy()
        if hit.empty:
            return
        for _, r in hit.iterrows():
            flags.append({
                "batch_id": batch_id,
                "record_type": record_type,
                "record_id": r.get(id_col),
                "flag_code": code,
                "severity": severity,
                "message": message,
                "vendor_canonical": r.get("vendor_canonical"),
                "vendor_id": r.get("vendor_id"),
                "date": r.get("date"),
                "amount_cents": r.get("amount_cents"),
                "source_file": r.get("source_file"),
                "source_row_number": r.get("source_row_number"),
                "row_hash": r.get("row_hash"),
                "created_at_utc": created_at,
            })

    # Missing field checks
    for df, rtype, idcol in [
        (fact_transactions, "transactions", "txn_id"),
        (fact_vendor_payments, "vendor_payments", "pay_id"),
    ]:
        if df.empty:
            continue
        add_flags(df, rtype, idcol, df["vendor_canonical"].isna() | (df["vendor_canonical"].astype(str).str.strip() == ""), "MISSING_VENDOR", "error", "Missing vendor after normalization.")
        add_flags(df, rtype, idcol, df["date"].isna() | (df["date"].astype(str).str.strip() == ""), "MISSING_DATE", "error", "Missing parsed date.")
        add_flags(df, rtype, idcol, df["amount_cents"].isna(), "MISSING_AMOUNT", "error", "Missing parsed amount_cents.")

    # Duplicate likely
    def dup(df: pd.DataFrame, rtype: str, idcol: str):
        if df.empty:
            return
        gcols = ["vendor_id", "date", "amount_cents"]
        base = df.dropna(subset=gcols)
        if base.empty:
            return
        counts = base.groupby(gcols)[idcol].transform("count")
        add_flags(base, rtype, idcol, counts > 1, "DUPLICATE_LIKELY", "warning", "Potential duplicate: same vendor, date, and amount.")
    dup(fact_transactions, "transactions", "txn_id")
    dup(fact_vendor_payments, "vendor_payments", "pay_id")

    # Weekend
    if not fact_transactions.empty and "is_weekend" in fact_transactions.columns:
        add_flags(fact_transactions, "transactions", "txn_id", fact_transactions["is_weekend"].astype(str) == "1", "WEEKEND_TRANSACTION", "info", "Transaction date is on a weekend.")

    # Outlier
    def outlier(df: pd.DataFrame, rtype: str, idcol: str):
        if df.empty:
            return
        s = _to_num(df["amount_cents"]).dropna()
        if len(s) == 0:
            return
        thr = 200000 if len(s) < 20 else float(np.percentile(s, 99))
        add_flags(df, rtype, idcol, _to_num(df["amount_cents"]) > thr, "AMOUNT_OUTLIER", "warning", f"Amount is unusually high (threshold: {int(thr)} cents).")
    outlier(fact_transactions, "transactions", "txn_id")
    outlier(fact_vendor_payments, "vendor_payments", "pay_id")

    # Policy rules
    def apply(df: pd.DataFrame, rtype: str, idcol: str):
        if df.empty:
            return
        for rule in policy_rules:
            if rule.applies_to not in ("both", rtype):
                continue
            if rule.field not in df.columns or not rule.op:
                continue
            cond = _compare(df[rule.field], rule.op, rule.value)
            msg = rule.message or f"Policy rule hit: {rule.field} {rule.op} {rule.value}"
            add_flags(df, rtype, idcol, cond, rule.flag_code, rule.severity, msg)
    apply(fact_transactions, "transactions", "txn_id")
    apply(fact_vendor_payments, "vendor_payments", "pay_id")

    return pd.DataFrame(flags)
