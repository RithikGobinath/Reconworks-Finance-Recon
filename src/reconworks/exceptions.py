from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .config import ProjectConfig
from .db import (
    connect,
    latest_batch_id,
    create_exceptions_runs_table,
    create_exceptions_table,
    insert_exception_run,
    delete_where_batch,
)
from .util import utc_now_iso, sha256_text, ensure_dir

def _mk_exception_id(batch_id: str, record_type: str, record_id: str, code: str, related: str = "") -> str:
    return sha256_text(f"{batch_id}|{record_type}|{record_id}|{code}|{related}")

def build_exceptions(
    batch_id: str,
    qa_flags: pd.DataFrame,
    fact_transactions: pd.DataFrame,
    fact_vendor_payments: pd.DataFrame,
    matches: pd.DataFrame,
    low_conf_threshold: float,
) -> pd.DataFrame:
    created_at = utc_now_iso()
    rows = []

    # 1) QA flags -> exceptions
    if qa_flags is not None and not qa_flags.empty:
        for _, r in qa_flags.iterrows():
            rid = str(r.get("record_id") or "")
            rtype = str(r.get("record_type") or "")
            code = str(r.get("flag_code") or "QA_FLAG")
            exid = _mk_exception_id(batch_id, rtype, rid, code, "")
            rows.append({
                "batch_id": batch_id,
                "exception_id": exid,
                "record_type": rtype,
                "record_id": rid,
                "related_record_id": "",
                "exception_code": code,
                "severity": str(r.get("severity") or "warning"),
                "message": str(r.get("message") or ""),
                "recommended_action": "Review and correct source data or mapping/normalization rules.",
                "vendor_canonical": r.get("vendor_canonical"),
                "vendor_id": r.get("vendor_id"),
                "date": r.get("date"),
                "amount_cents": r.get("amount_cents"),
                "created_at_utc": created_at,
            })

    matched_txn = set(matches["txn_id"].tolist()) if matches is not None and not matches.empty else set()
    matched_pay = set(matches["pay_id"].tolist()) if matches is not None and not matches.empty else set()

    # 2) Unmatched transaction facts
    if fact_transactions is not None and not fact_transactions.empty:
        um_tx = fact_transactions[~fact_transactions["txn_id"].isin(matched_txn)].copy()
        for _, r in um_tx.iterrows():
            rid = str(r["txn_id"])
            code = "UNMATCHED_TRANSACTION"
            exid = _mk_exception_id(batch_id, "transactions", rid, code, "")
            rows.append({
                "batch_id": batch_id,
                "exception_id": exid,
                "record_type": "transactions",
                "record_id": rid,
                "related_record_id": "",
                "exception_code": code,
                "severity": "warning",
                "message": "No matching vendor payment found.",
                "recommended_action": "Investigate: missing payment, timing difference, amount mismatch, or vendor normalization gap.",
                "vendor_canonical": r.get("vendor_canonical"),
                "vendor_id": r.get("vendor_id"),
                "date": r.get("date"),
                "amount_cents": r.get("amount_cents"),
                "created_at_utc": created_at,
            })

    # 3) Unmatched payments
    if fact_vendor_payments is not None and not fact_vendor_payments.empty:
        um_pay = fact_vendor_payments[~fact_vendor_payments["pay_id"].isin(matched_pay)].copy()
        for _, r in um_pay.iterrows():
            rid = str(r["pay_id"])
            code = "UNMATCHED_VENDOR_PAYMENT"
            exid = _mk_exception_id(batch_id, "vendor_payments", rid, code, "")
            rows.append({
                "batch_id": batch_id,
                "exception_id": exid,
                "record_type": "vendor_payments",
                "record_id": rid,
                "related_record_id": "",
                "exception_code": code,
                "severity": "warning",
                "message": "No matching transaction found.",
                "recommended_action": "Investigate: missing transaction feed, timing difference, amount mismatch, or vendor normalization gap.",
                "vendor_canonical": r.get("vendor_canonical"),
                "vendor_id": r.get("vendor_id"),
                "date": r.get("date"),
                "amount_cents": r.get("amount_cents"),
                "created_at_utc": created_at,
            })

    # 4) Low-confidence matches
    if matches is not None and not matches.empty:
        low = matches[matches["match_score"].astype(float) < float(low_conf_threshold)].copy()
        for _, r in low.iterrows():
            rid = str(r["txn_id"])
            related = str(r["pay_id"])
            code = "LOW_CONFIDENCE_MATCH"
            exid = _mk_exception_id(batch_id, "transactions", rid, code, related)
            rows.append({
                "batch_id": batch_id,
                "exception_id": exid,
                "record_type": "transactions",
                "record_id": rid,
                "related_record_id": related,
                "exception_code": code,
                "severity": "warning",
                "message": f"Matched but low confidence (score={float(r['match_score']):.3f}).",
                "recommended_action": "Review candidate details; confirm or adjust matching thresholds/rules.",
                "vendor_canonical": None,
                "vendor_id": None,
                "date": None,
                "amount_cents": None,
                "created_at_utc": created_at,
            })

    return pd.DataFrame(rows)

def exceptions_all(repo_root: Path, cfg: ProjectConfig, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    conn = connect(repo_root / cfg.database_path)
    create_exceptions_runs_table(conn)
    create_exceptions_table(conn)

    b = batch_id or latest_batch_id(conn)
    if not b:
        conn.close()
        return {"exceptions": 0}

    qa = pd.read_sql_query("SELECT * FROM qa_flags WHERE batch_id=?", conn, params=(b,))
    ft = pd.read_sql_query("SELECT * FROM fact_transactions WHERE batch_id=?", conn, params=(b,))
    fp = pd.read_sql_query("SELECT * FROM fact_vendor_payments WHERE batch_id=?", conn, params=(b,))
    matches = pd.read_sql_query("SELECT * FROM matches WHERE batch_id=?", conn, params=(b,))

    exc = build_exceptions(
        batch_id=b,
        qa_flags=qa,
        fact_transactions=ft,
        fact_vendor_payments=fp,
        matches=matches,
        low_conf_threshold=cfg.matching.low_confidence_threshold,
    )

    delete_where_batch(conn, "exceptions", b)
    delete_where_batch(conn, "exception_runs", b)

    if not exc.empty:
        exc.to_sql("exceptions", conn, if_exists="append", index=False)

    insert_exception_run(conn, {
        "created_at_utc": utc_now_iso(),
        "batch_id": b,
        "exception_count": int(len(exc)),
    })

    if export_csv:
        exc.to_csv(out_dir / "csv" / "exceptions.csv", index=False)

    conn.close()
    return {"exceptions": int(len(exc))}
