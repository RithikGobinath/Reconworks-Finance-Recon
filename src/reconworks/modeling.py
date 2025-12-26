from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from .config import ProjectConfig
from .db import (
    connect,
    table_exists,
    latest_batch_id,
    create_modeling_runs_table,
    insert_modeling_run,
)
from .util import utc_now_iso, sha256_text, ensure_dir

def _ensure_dim_vendor(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_vendor (
            vendor_id TEXT PRIMARY KEY,
            vendor_canonical TEXT UNIQUE,
            created_at_utc TEXT
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_vendor_canon ON dim_vendor(vendor_canonical);")
    conn.commit()

def _ensure_fact_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_transactions (
            txn_id TEXT PRIMARY KEY,
            batch_id TEXT,
            row_hash TEXT,
            source_file TEXT,
            source_row_number INTEGER,
            date TEXT,
            month TEXT,
            year TEXT,
            is_weekend INTEGER,
            amount_cents INTEGER,
            currency TEXT,
            vendor_id TEXT,
            vendor_canonical TEXT,
            vendor_clean TEXT,
            vendor_raw TEXT,
            clean_status TEXT,
            clean_notes TEXT,
            vendor_norm_method TEXT,
            vendor_norm_confidence REAL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_vendor_payments (
            pay_id TEXT PRIMARY KEY,
            batch_id TEXT,
            row_hash TEXT,
            source_file TEXT,
            source_row_number INTEGER,
            date TEXT,
            month TEXT,
            year TEXT,
            is_weekend INTEGER,
            amount_cents INTEGER,
            currency TEXT,
            vendor_id TEXT,
            vendor_canonical TEXT,
            vendor_clean TEXT,
            vendor_raw TEXT,
            clean_status TEXT,
            clean_notes TEXT,
            vendor_norm_method TEXT,
            vendor_norm_confidence REAL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_tx_batch ON fact_transactions(batch_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_vp_batch ON fact_vendor_payments(batch_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_tx_vendor ON fact_transactions(vendor_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_vp_vendor ON fact_vendor_payments(vendor_id);")
    conn.commit()

def _vendor_id(canonical: str) -> str:
    return sha256_text(f"vendor|{canonical.strip().lower()}")

def _derive_date_fields(df: pd.DataFrame) -> pd.DataFrame:
    # Expect df['date'] as ISO yyyy-mm-dd or empty
    dt = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = dt.dt.strftime("%Y-%m").fillna("")
    df["year"] = dt.dt.strftime("%Y").fillna("")
    # weekday: Monday=0, Sunday=6
    df["is_weekend"] = dt.dt.weekday.isin([5, 6]).fillna(False).astype(int)
    return df

def model_all(repo_root: Path, cfg: ProjectConfig, export_csv: bool = False, batch_id: Optional[str] = None) -> Dict[str, int]:
    """Stage 5: create dim_vendor + fact tables from normalized data."""
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    db_path = repo_root / cfg.database_path
    conn = connect(db_path)
    create_modeling_runs_table(conn)
    _ensure_dim_vendor(conn)
    _ensure_fact_tables(conn)

    if batch_id is None:
        batch_id = latest_batch_id(conn)
    if batch_id is None:
        conn.close()
        return {"transactions": 0, "vendor_payments": 0}

    modeled_at = utc_now_iso()
    summary: Dict[str, int] = {}

    # Load normalized inputs
    inputs = {
        "transactions": ("norm_transactions", "fact_transactions"),
        "vendor_payments": ("norm_vendor_payments", "fact_vendor_payments"),
    }

    # Make modeling idempotent per batch
    conn.execute("DELETE FROM fact_transactions WHERE batch_id = ?", (batch_id,))
    conn.execute("DELETE FROM fact_vendor_payments WHERE batch_id = ?", (batch_id,))
    conn.execute("DELETE FROM modeling_runs WHERE batch_id = ?", (batch_id,))
    conn.commit()

    # Preload dim_vendor mapping
    def refresh_vendor_map() -> Dict[str, str]:
        rows = conn.execute("SELECT vendor_canonical, vendor_id FROM dim_vendor;").fetchall()
        return {r[0]: r[1] for r in rows}

    vendor_map = refresh_vendor_map()

    for source_name, (in_table, out_table) in inputs.items():
        if not table_exists(conn, in_table):
            summary[source_name] = 0
            continue

        df = pd.read_sql_query(f"SELECT * FROM {in_table} WHERE batch_id = ?", conn, params=(batch_id,))
        if df.empty:
            summary[source_name] = 0
            continue

        # Deduplicate within batch if user re-ran earlier stages unexpectedly
        if "row_hash" in df.columns:
            df = df.drop_duplicates(subset=["row_hash"], keep="first")

        # Ensure vendor dimension rows exist
        canon = df.get("vendor_canonical")
        canon_vals = sorted({str(x) for x in canon.dropna().tolist() if str(x).strip()})
        new_rows = []
        for c in canon_vals:
            if c not in vendor_map:
                new_rows.append((_vendor_id(c), c, modeled_at))
        if new_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO dim_vendor (vendor_id, vendor_canonical, created_at_utc) VALUES (?, ?, ?);",
                new_rows,
            )
            conn.commit()
            vendor_map = refresh_vendor_map()

        df["vendor_id"] = df["vendor_canonical"].map(vendor_map).fillna("")

        # Derive date fields
        if "date" not in df.columns:
            df["date"] = ""
        df = _derive_date_fields(df)

        # Currency: optional, default USD
        if "currency" not in df.columns:
            df["currency"] = "USD"
        df["currency"] = df["currency"].replace("", "USD")

        # Amount cents: ensure integer or null
        if "amount_cents" in df.columns:
            df["amount_cents"] = pd.to_numeric(df["amount_cents"], errors="coerce").fillna(0).astype(int)
        else:
            df["amount_cents"] = 0

        # Build IDs and select columns
        if source_name == "transactions":
            df["txn_id"] = df.apply(lambda r: sha256_text(f"{batch_id}|txn|{r.get('row_hash','')}|{r.get('source_row_number','')}"), axis=1)
            cols = [
                "txn_id","batch_id","row_hash","source_file","source_row_number",
                "date","month","year","is_weekend","amount_cents","currency",
                "vendor_id","vendor_canonical","vendor_clean","vendor_raw",
                "clean_status","clean_notes","vendor_norm_method","vendor_norm_confidence"
            ]
            # ensure all exist
            for c in cols:
                if c not in df.columns:
                    df[c] = "" if c not in ["is_weekend","amount_cents"] else 0
            out_df = df[cols].copy()
            # Insert
            placeholders = ",".join(["?"]*len(cols))
            conn.executemany(
                f"INSERT INTO fact_transactions ({','.join(cols)}) VALUES ({placeholders});",
                out_df.itertuples(index=False, name=None),
            )
            conn.commit()
            summary[source_name] = len(out_df)
            insert_modeling_run(conn, {
                "modeled_at_utc": modeled_at,
                "batch_id": batch_id,
                "source_name": source_name,
                "input_table": in_table,
                "output_table": out_table,
                "row_count": int(len(out_df)),
                "distinct_vendor_count": int(out_df["vendor_id"].nunique()),
            })
            if export_csv:
                out_df.to_csv(out_dir / "csv" / "fact_transactions.csv", index=False)

        else:
            df["pay_id"] = df.apply(lambda r: sha256_text(f"{batch_id}|pay|{r.get('row_hash','')}|{r.get('source_row_number','')}"), axis=1)
            cols = [
                "pay_id","batch_id","row_hash","source_file","source_row_number",
                "date","month","year","is_weekend","amount_cents","currency",
                "vendor_id","vendor_canonical","vendor_clean","vendor_raw",
                "clean_status","clean_notes","vendor_norm_method","vendor_norm_confidence"
            ]
            for c in cols:
                if c not in df.columns:
                    df[c] = "" if c not in ["is_weekend","amount_cents"] else 0
            out_df = df[cols].copy()
            placeholders = ",".join(["?"]*len(cols))
            conn.executemany(
                f"INSERT INTO fact_vendor_payments ({','.join(cols)}) VALUES ({placeholders});",
                out_df.itertuples(index=False, name=None),
            )
            conn.commit()
            summary[source_name] = len(out_df)
            insert_modeling_run(conn, {
                "modeled_at_utc": modeled_at,
                "batch_id": batch_id,
                "source_name": source_name,
                "input_table": in_table,
                "output_table": out_table,
                "row_count": int(len(out_df)),
                "distinct_vendor_count": int(out_df["vendor_id"].nunique()),
            })
            if export_csv:
                out_df.to_csv(out_dir / "csv" / "fact_vendor_payments.csv", index=False)

    conn.close()
    return summary
