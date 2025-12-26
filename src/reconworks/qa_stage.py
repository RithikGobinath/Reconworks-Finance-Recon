from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .config import ProjectConfig
from .db import (
    connect,
    create_qa_runs_table,
    create_qa_flags_table,
    latest_batch_id,
    delete_where_batch,
)
from .qa_checks import load_policy_rules, run_qa_for_batch
from .util import utc_now_iso, ensure_dir

def qa_all(repo_root: Path, cfg: ProjectConfig, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    conn = connect(repo_root / cfg.database_path)
    create_qa_runs_table(conn)
    create_qa_flags_table(conn)

    b = batch_id or latest_batch_id(conn)
    if not b:
        conn.close()
        return {"qa_flags": 0}

    ft = pd.read_sql_query("SELECT * FROM fact_transactions WHERE batch_id=?", conn, params=(b,))
    fp = pd.read_sql_query("SELECT * FROM fact_vendor_payments WHERE batch_id=?", conn, params=(b,))

    rules = load_policy_rules(repo_root / cfg.policy_rules_path)

    flags = run_qa_for_batch(batch_id=b, fact_transactions=ft, fact_vendor_payments=fp, policy_rules=rules)

    # Idempotent per batch
    delete_where_batch(conn, "qa_flags", b)
    delete_where_batch(conn, "qa_runs", b)

    if not flags.empty:
        flags.to_sql("qa_flags", conn, if_exists="append", index=False)

    conn.execute(
        "INSERT INTO qa_runs (batch_id, qa_at_utc, policy_rules_path) VALUES (?, ?, ?)",
        (b, utc_now_iso(), cfg.policy_rules_path),
    )
    conn.commit()

    if export_csv:
        flags.to_csv(out_dir / "csv" / "qa_flags.csv", index=False)

    conn.close()
    return {"qa_flags": int(len(flags))}
