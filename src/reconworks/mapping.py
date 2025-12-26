from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, List

import pandas as pd

from .config import ProjectConfig, MappingConfig
from .db import connect, table_exists, add_columns_text, create_mapping_runs_table, insert_mapping_run, latest_batch_id
from .util import utc_now_iso, sanitize_column, ensure_dir

CANON_COLS = [
    "vendor_raw",
    "date_raw",
    "amount_raw",
    "map_vendor_from",
    "map_date_from",
    "map_amount_from",
    "mapping_status",
    "mapping_notes",
]

def _pick_column(existing_cols: List[str], candidates: List[str]) -> Optional[str]:
    """Pick the first candidate that exists in the ingested (sanitized) columns."""
    # candidates may be given as human names; sanitize them to match ingest output
    sanitized = [sanitize_column(c) for c in candidates]
    existing_set = set(existing_cols)
    for c in sanitized:
        if c in existing_set:
            return c
    # also allow direct match (already sanitized in config)
    for c in candidates:
        if c in existing_set:
            return c
    return None

def map_source(
    conn,
    batch_id: str,
    source_name: str,
    mapping: Optional[MappingConfig],
    output_dir: Path,
    export_csv: bool = False,
) -> int:
    in_table = f"stg_{source_name}_raw"
    out_table = f"stg_{source_name}_mapped"

    if not table_exists(conn, in_table):
        return 0

    df = pd.read_sql_query(
        f"SELECT * FROM {in_table} WHERE batch_id = ?",
        conn,
        params=[batch_id],
    )

    if df.empty:
        return 0

    existing_cols = list(df.columns)

    vendor_col = date_col = amount_col = None
    if mapping:
        vendor_col = _pick_column(existing_cols, mapping.vendor_raw)
        date_col = _pick_column(existing_cols, mapping.date_raw)
        amount_col = _pick_column(existing_cols, mapping.amount_raw)

    # Heuristic fallbacks
    if vendor_col is None:
        vendor_col = _pick_column(existing_cols, ["vendor_raw", "merchant", "vendor", "payee", "supplier"])
    if date_col is None:
        date_col = _pick_column(existing_cols, ["date_raw", "post_date", "entry_date", "date", "transaction_date"])
    if amount_col is None:
        amount_col = _pick_column(existing_cols, ["amount_raw", "amount", "amt", "payment_amount", "transaction_amount"])

    notes = []
    status = "ok"
    if vendor_col is None:
        status = "error"
        notes.append("Missing vendor column mapping")
    if date_col is None:
        status = "error"
        notes.append("Missing date column mapping")
    if amount_col is None:
        status = "error"
        notes.append("Missing amount column mapping")

    df["vendor_raw"] = df[vendor_col] if vendor_col else ""
    df["date_raw"] = df[date_col] if date_col else ""
    df["amount_raw"] = df[amount_col] if amount_col else ""

    df["map_vendor_from"] = vendor_col or ""
    df["map_date_from"] = date_col or ""
    df["map_amount_from"] = amount_col or ""
    df["mapping_status"] = status
    df["mapping_notes"] = "; ".join(notes)

    if table_exists(conn, out_table):
        # Make mapping idempotent for a given batch_id: re-running `map` replaces that batch’s output.
        conn.execute(f"DELETE FROM {out_table} WHERE batch_id = ?", (batch_id,))
        conn.commit()
        add_columns_text(conn, out_table, df.columns)

    df.to_sql(out_table, conn, if_exists="append", index=False)

    if export_csv:
        ensure_dir(output_dir / "csv")
        df.to_csv(output_dir / "csv" / f"{out_table}.csv", index=False)

    create_mapping_runs_table(conn)
    insert_mapping_run(
        conn,
        {
            "mapped_at_utc": utc_now_iso(),
            "batch_id": batch_id,
            "source_name": source_name,
            "input_table": in_table,
            "output_table": out_table,
            "vendor_col": vendor_col or "",
            "date_col": date_col or "",
            "amount_col": amount_col or "",
            "row_count": int(len(df)),
            "notes": "; ".join(notes),
        },
    )

    return int(len(df))

def map_all(
    repo_root: Path,
    cfg: ProjectConfig,
    batch_id: Optional[str] = None,
    export_csv: bool = False,
) -> Dict[str, int]:
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    conn = connect(repo_root / cfg.database_path)

    if batch_id is None:
        batch_id = latest_batch_id(conn)
        if batch_id is None:
            conn.close()
            raise RuntimeError("No batches found. Run Stage 1 ingest first.")

    results: Dict[str, int] = {}
    for source_name, source_cfg in cfg.sources.items():
        results[source_name] = map_source(
            conn=conn,
            batch_id=batch_id,
            source_name=source_name,
            mapping=source_cfg.mapping,
            output_dir=out_dir,
            export_csv=export_csv,
        )

    conn.close()
    return results
