from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from .config import ProjectConfig
from .db import (
    connect,
    table_exists,
    add_columns_text,
    latest_batch_id,
    create_cleaning_runs_table,
    insert_cleaning_run,
)
from .util import utc_now_iso, ensure_dir


def _parse_date_iso(value: str) -> Tuple[Optional[str], str]:
    s = (value or "").strip()
    if not s:
        return None, "empty"
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None, "invalid"
    return dt.date().isoformat(), "ok"


def _parse_amount_cents(value: str) -> Tuple[Optional[int], str]:
    s = (value or "").strip()
    if not s:
        return None, "empty"

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = s.replace(",", "")
    for sym in ["$", "€", "£", "₹"]:
        s = s.replace(sym, "")
    s = s.replace("USD", "").replace("usd", "").strip()

    if s.startswith("-"):
        neg = True
        s = s[1:].strip()
    elif s.startswith("+"):
        s = s[1:].strip()

    try:
        d = Decimal(s)
    except (InvalidOperation, ValueError):
        return None, "invalid"

    cents = int((d * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    if neg:
        cents = -cents
    return cents, "ok"


def clean_source(
    conn,
    batch_id: str,
    source_name: str,
    export_csv: bool,
    output_dir: Path,
) -> int:
    in_table = f"stg_{source_name}_mapped"
    out_table = f"clean_{source_name}"

    if not table_exists(conn, in_table):
        return 0

    df = pd.read_sql_query(
        f"SELECT * FROM {in_table} WHERE batch_id = ?",
        conn,
        params=[batch_id],
    )

    if df.empty:
        return 0

    # Deduplicate within batch in case mapping was re-run.
    if "row_hash" in df.columns:
        df = df.drop_duplicates(subset=["row_hash"], keep="first")

    # Parse raw fields
    date_vals = []
    date_status = []
    amt_vals = []
    amt_status = []
    notes = []

    for _, row in df.iterrows():
        d, ds = _parse_date_iso(str(row.get("date_raw", "")))
        a, astatus = _parse_amount_cents(str(row.get("amount_raw", "")))

        date_vals.append(d or "")
        date_status.append(ds)
        amt_vals.append(a)
        amt_status.append(astatus)

        n = []
        if ds != "ok":
            n.append(f"date:{ds}")
        if astatus != "ok":
            n.append(f"amount:{astatus}")
        notes.append("; ".join(n))

    df["date"] = date_vals
    df["date_parse_status"] = date_status
    df["amount_cents"] = amt_vals
    df["amount_parse_status"] = amt_status
    df["clean_status"] = ["ok" if (ds == "ok" and a == "ok") else "error" for ds, a in zip(date_status, amt_status)]
    df["clean_notes"] = notes

    # Idempotent output per batch
    if table_exists(conn, out_table):
        conn.execute(f"DELETE FROM {out_table} WHERE batch_id = ?", (batch_id,))
        conn.commit()
        add_columns_text(conn, out_table, df.columns)

    df.to_sql(out_table, conn, if_exists="append", index=False)

    if export_csv:
        ensure_dir(output_dir / "csv")
        df.to_csv(output_dir / "csv" / f"{out_table}.csv", index=False)

    create_cleaning_runs_table(conn)
    insert_cleaning_run(conn, {
        "cleaned_at_utc": utc_now_iso(),
        "batch_id": batch_id,
        "source_name": source_name,
        "input_table": in_table,
        "output_table": out_table,
        "row_count": int(len(df)),
        "error_count": int((df["clean_status"] != "ok").sum()),
    })

    return int(len(df))


def clean_all(
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
    for source_name in cfg.sources.keys():
        results[source_name] = clean_source(
            conn=conn,
            batch_id=batch_id,
            source_name=source_name,
            export_csv=export_csv,
            output_dir=out_dir,
        )

    conn.close()
    return results
