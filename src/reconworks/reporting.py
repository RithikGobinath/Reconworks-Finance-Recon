from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .config import ProjectConfig
from .db import (
    connect,
    latest_batch_id,
    create_reporting_runs_table,
    insert_report_run,
    delete_where_batch,
)
from .util import utc_now_iso, ensure_dir

def _write_table(conn, name: str, df: pd.DataFrame, batch_id: str) -> None:
    # Replace only this batch if table has batch_id column; else drop and recreate is overkill.
    if "batch_id" in df.columns:
        delete_where_batch(conn, name, batch_id)
    # Ensure table exists by writing (pandas will create)
    df.to_sql(name, conn, if_exists="append", index=False)

def reports_all(repo_root: Path, cfg: ProjectConfig, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    conn = connect(repo_root / cfg.database_path)
    create_reporting_runs_table(conn)

    b = batch_id or latest_batch_id(conn)
    if not b:
        conn.close()
        return {"reports": 0}

    ft = pd.read_sql_query("SELECT * FROM fact_transactions WHERE batch_id=?", conn, params=(b,))
    matches = pd.read_sql_query("SELECT * FROM matches WHERE batch_id=?", conn, params=(b,))
    exc = pd.read_sql_query("SELECT * FROM exceptions WHERE batch_id=?", conn, params=(b,))

    if ft.empty:
        conn.close()
        return {"reports": 0}

    ft["is_matched"] = ft["txn_id"].isin(set(matches["txn_id"].tolist())) if not matches.empty else False
    ft["amount_usd"] = ft["amount_cents"].astype(float) / 100.0

    # Spend by month + vendor
    spend = (
        ft.groupby(["batch_id","month","vendor_canonical"], dropna=False)
          .agg(txn_count=("txn_id","count"),
               matched_count=("is_matched","sum"),
               spend_usd=("amount_usd","sum"))
          .reset_index()
          .sort_values(["month","spend_usd"], ascending=[True, False])
    )

    # Match rate by month
    match_rate = (
        ft.groupby(["batch_id","month"])
          .agg(txn_count=("txn_id","count"),
               matched_count=("is_matched","sum"),
               spend_usd=("amount_usd","sum"))
          .reset_index()
    )
    match_rate["match_rate"] = (match_rate["matched_count"] / match_rate["txn_count"]).round(4)

    # Exceptions by code
    exc_by = pd.DataFrame(columns=["batch_id","exception_code","severity","exception_count"])
    if not exc.empty:
        exc_by = (
            exc.groupby(["batch_id","exception_code","severity"])
               .size()
               .reset_index(name="exception_count")
               .sort_values(["exception_count"], ascending=False)
        )

    # Top vendors (for dashboard)
    top_n = int(cfg.reporting.top_n_vendors)
    top_vendors = (
        ft.groupby(["batch_id","vendor_canonical"])
          .agg(spend_usd=("amount_usd","sum"), txn_count=("txn_id","count"))
          .reset_index()
          .sort_values("spend_usd", ascending=False)
          .head(top_n)
    )

    # Idempotent: delete + write tables
    for table_name in ["rpt_spend_by_month_vendor","rpt_match_rate_by_month","rpt_exceptions_by_code","rpt_top_vendors"]:
        # create empty table by writing empty df is annoying; just delete if exists and then write.
        try:
            delete_where_batch(conn, table_name, b)
        except Exception:
            pass

    if not spend.empty:
        spend.to_sql("rpt_spend_by_month_vendor", conn, if_exists="append", index=False)
    if not match_rate.empty:
        match_rate.to_sql("rpt_match_rate_by_month", conn, if_exists="append", index=False)
    if not exc_by.empty:
        exc_by.to_sql("rpt_exceptions_by_code", conn, if_exists="append", index=False)
    if not top_vendors.empty:
        top_vendors.to_sql("rpt_top_vendors", conn, if_exists="append", index=False)

    # report_runs idempotent per batch
    delete_where_batch(conn, "report_runs", b)
    insert_report_run(conn, {"created_at_utc": utc_now_iso(), "batch_id": b})

    if export_csv:
        spend.to_csv(out_dir / "csv" / "rpt_spend_by_month_vendor.csv", index=False)
        match_rate.to_csv(out_dir / "csv" / "rpt_match_rate_by_month.csv", index=False)
        exc_by.to_csv(out_dir / "csv" / "rpt_exceptions_by_code.csv", index=False)
        top_vendors.to_csv(out_dir / "csv" / "rpt_top_vendors.csv", index=False)

    conn.close()
    return {
        "rpt_spend_by_month_vendor": int(len(spend)),
        "rpt_match_rate_by_month": int(len(match_rate)),
        "rpt_exceptions_by_code": int(len(exc_by)),
        "rpt_top_vendors": int(len(top_vendors)),
    }
