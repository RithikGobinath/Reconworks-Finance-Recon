from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz

from .config import ProjectConfig
from .db import (
    connect,
    latest_batch_id,
    create_match_candidates_table,
    create_matches_table,
    create_matching_runs_table,
    delete_where_batch,
    insert_matching_run,
)
from .util import utc_now_iso, ensure_dir

def _to_dt(s: pd.Series) -> pd.Series:
    # Expect ISO date YYYY-MM-DD
    return pd.to_datetime(s, errors="coerce")

def _vendor_similarity(a: str, b: str) -> float:
    a = (a or "").strip()
    b = (b or "").strip()
    if not a or not b:
        return 0.0
    if a.lower() == b.lower():
        return 1.0
    # token_set_ratio is robust to extra tokens in one string
    return float(fuzz.token_set_ratio(a, b)) / 100.0

def _score(vendor_sim: float, date_diff_days: int, date_window_days: int, amount_diff_cents: int, amount_tolerance_cents: int,
           w_vendor: float, w_date: float, w_amount: float) -> float:
    if date_window_days <= 0:
        date_sim = 1.0 if date_diff_days == 0 else 0.0
    else:
        date_sim = max(0.0, 1.0 - (abs(date_diff_days) / float(date_window_days)))
    if amount_tolerance_cents <= 0:
        amount_sim = 1.0 if amount_diff_cents == 0 else 0.0
    else:
        amount_sim = max(0.0, 1.0 - (abs(amount_diff_cents) / float(amount_tolerance_cents)))
    score = (w_vendor * vendor_sim) + (w_date * date_sim) + (w_amount * amount_sim)
    return float(round(score, 6))

def _match_type(vendor_sim: float, date_diff_days: int, amount_diff_cents: int) -> str:
    if vendor_sim >= 0.999 and date_diff_days == 0 and amount_diff_cents == 0:
        return "exact"
    if vendor_sim >= 0.999 and amount_diff_cents == 0 and abs(date_diff_days) <= 1:
        return "date_window"
    if vendor_sim >= 0.90 and amount_diff_cents == 0:
        return "vendor_fuzzy"
    return "weak"

def build_candidates(
    batch_id: str,
    fact_transactions: pd.DataFrame,
    fact_vendor_payments: pd.DataFrame,
    date_window_days: int,
    amount_tolerance_cents: int,
    w_vendor: float,
    w_date: float,
    w_amount: float,
) -> pd.DataFrame:
    if fact_transactions.empty or fact_vendor_payments.empty:
        return pd.DataFrame(columns=[
            "batch_id","txn_id","pay_id","vendor_sim","date_diff_days","amount_diff_cents","score"
        ])

    tx = fact_transactions.copy()
    pay = fact_vendor_payments.copy()

    tx["date_dt"] = _to_dt(tx["date"])
    pay["date_dt"] = _to_dt(pay["date"])

    tx = tx.dropna(subset=["date_dt", "amount_cents"])
    pay = pay.dropna(subset=["date_dt", "amount_cents"])

    rows = []
    for _, t in tx.iterrows():
        t_amount = int(t["amount_cents"])
        t_date = t["date_dt"]
        # blocking filter
        p = pay
        if date_window_days > 0:
            lo = t_date - pd.Timedelta(days=date_window_days)
            hi = t_date + pd.Timedelta(days=date_window_days)
            p = p[(p["date_dt"] >= lo) & (p["date_dt"] <= hi)]
        if amount_tolerance_cents == 0:
            p = p[p["amount_cents"].astype(int) == t_amount]
        else:
            p = p[(p["amount_cents"].astype(int) - t_amount).abs() <= amount_tolerance_cents]

        if p.empty:
            continue

        for _, pr in p.iterrows():
            vendor_sim = _vendor_similarity(str(t.get("vendor_canonical","")), str(pr.get("vendor_canonical","")))
            date_diff = int((pr["date_dt"] - t_date).days)
            amount_diff = int(int(pr["amount_cents"]) - t_amount)
            score = _score(vendor_sim, date_diff, date_window_days, amount_diff, amount_tolerance_cents, w_vendor, w_date, w_amount)
            rows.append({
                "batch_id": batch_id,
                "txn_id": t["txn_id"],
                "pay_id": pr["pay_id"],
                "vendor_sim": vendor_sim,
                "date_diff_days": date_diff,
                "amount_diff_cents": amount_diff,
                "score": score,
            })

    return pd.DataFrame(rows)

def choose_matches(
    candidates: pd.DataFrame,
    min_score: float,
) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(columns=[
            "batch_id","txn_id","pay_id","match_score","match_type","vendor_sim","date_diff_days","amount_diff_cents","matched_at_utc"
        ])

    cand = candidates.sort_values(["score","vendor_sim"], ascending=[False, False]).reset_index(drop=True)
    used_txn = set()
    used_pay = set()
    chosen = []
    matched_at = utc_now_iso()

    for _, r in cand.iterrows():
        if float(r["score"]) < float(min_score):
            break
        txn_id = r["txn_id"]
        pay_id = r["pay_id"]
        if txn_id in used_txn or pay_id in used_pay:
            continue
        used_txn.add(txn_id)
        used_pay.add(pay_id)

        vendor_sim = float(r["vendor_sim"])
        date_diff = int(r["date_diff_days"])
        amount_diff = int(r["amount_diff_cents"])
        chosen.append({
            "batch_id": r["batch_id"],
            "txn_id": txn_id,
            "pay_id": pay_id,
            "match_score": float(r["score"]),
            "match_type": _match_type(vendor_sim, date_diff, amount_diff),
            "vendor_sim": vendor_sim,
            "date_diff_days": date_diff,
            "amount_diff_cents": amount_diff,
            "matched_at_utc": matched_at,
        })

    return pd.DataFrame(chosen)

def match_all(repo_root: Path, cfg: ProjectConfig, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    conn = connect(repo_root / cfg.database_path)
    create_match_candidates_table(conn)
    create_matches_table(conn)
    create_matching_runs_table(conn)

    b = batch_id or latest_batch_id(conn)
    if not b:
        conn.close()
        return {"matches": 0, "unmatched_transactions": 0, "unmatched_vendor_payments": 0}

    ft = pd.read_sql_query("SELECT * FROM fact_transactions WHERE batch_id=?", conn, params=(b,))
    fp = pd.read_sql_query("SELECT * FROM fact_vendor_payments WHERE batch_id=?", conn, params=(b,))

    mcfg = cfg.matching
    candidates = build_candidates(
        batch_id=b,
        fact_transactions=ft,
        fact_vendor_payments=fp,
        date_window_days=mcfg.date_window_days,
        amount_tolerance_cents=mcfg.amount_tolerance_cents,
        w_vendor=mcfg.vendor_weight,
        w_date=mcfg.date_weight,
        w_amount=mcfg.amount_weight,
    )
    matches = choose_matches(candidates, min_score=mcfg.min_score)

    matched_txn = set(matches["txn_id"].tolist()) if not matches.empty else set()
    matched_pay = set(matches["pay_id"].tolist()) if not matches.empty else set()
    unmatched_tx = ft[~ft["txn_id"].isin(matched_txn)].copy() if not ft.empty else ft
    unmatched_pay = fp[~fp["pay_id"].isin(matched_pay)].copy() if not fp.empty else fp

    # Idempotent per batch
    delete_where_batch(conn, "match_candidates", b)
    delete_where_batch(conn, "matches", b)
    delete_where_batch(conn, "matching_runs", b)

    if not candidates.empty:
        candidates.to_sql("match_candidates", conn, if_exists="append", index=False)
    if not matches.empty:
        matches.to_sql("matches", conn, if_exists="append", index=False)

    insert_matching_run(conn, {
        "matched_at_utc": utc_now_iso(),
        "batch_id": b,
        "date_window_days": mcfg.date_window_days,
        "amount_tolerance_cents": mcfg.amount_tolerance_cents,
        "min_score": mcfg.min_score,
        "match_count": int(len(matches)),
        "unmatched_tx_count": int(len(unmatched_tx)),
        "unmatched_pay_count": int(len(unmatched_pay)),
    })

    if export_csv:
        candidates.to_csv(out_dir / "csv" / "match_candidates.csv", index=False)
        matches.to_csv(out_dir / "csv" / "matches.csv", index=False)
        unmatched_tx.to_csv(out_dir / "csv" / "unmatched_transactions.csv", index=False)
        unmatched_pay.to_csv(out_dir / "csv" / "unmatched_vendor_payments.csv", index=False)

    conn.close()
    return {
        "matches": int(len(matches)),
        "unmatched_transactions": int(len(unmatched_tx)),
        "unmatched_vendor_payments": int(len(unmatched_pay)),
        "candidates": int(len(candidates)),
    }
