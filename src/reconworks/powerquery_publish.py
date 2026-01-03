from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .config import ProjectConfig
from .db import connect, latest_batch_id
from .util import ensure_dir, utc_now_compact

@dataclass(frozen=True)
class PublishResult:
    files_written: int
    output_dirs: List[str]

def _export_query(conn, sql: str, params: tuple, out_path: Path) -> None:
    df = pd.read_sql_query(sql, conn, params=params)
    df.to_csv(out_path, index=False)

def publish_powerquery_drop(
    repo_root: Path,
    cfg: ProjectConfig,
    batch_id: Optional[str],
    drop_root: str,
    mode: str = "history",  # "history" or "latest"
) -> PublishResult:
    out_root = repo_root / drop_root
    conn = connect(repo_root / cfg.database_path)
    b = batch_id or latest_batch_id(conn)
    if not b:
        conn.close()
        return PublishResult(files_written=0, output_dirs=[])

    ts = utc_now_compact()

    datasets = [
        ("exceptions", "SELECT * FROM exceptions WHERE batch_id=?", (b,)),
        ("matches", "SELECT * FROM matches WHERE batch_id=?", (b,)),
        ("qa_flags", "SELECT * FROM qa_flags WHERE batch_id=?", (b,)),
        ("rpt_spend_by_month_vendor", "SELECT * FROM rpt_spend_by_month_vendor WHERE batch_id=?", (b,)),
        ("rpt_match_rate_by_month", "SELECT * FROM rpt_match_rate_by_month WHERE batch_id=?", (b,)),
        ("rpt_exceptions_by_code", "SELECT * FROM rpt_exceptions_by_code WHERE batch_id=?", (b,)),
        ("rpt_top_vendors", "SELECT * FROM rpt_top_vendors WHERE batch_id=?", (b,)),
    ]

    files_written = 0
    out_dirs: List[str] = []

    if mode == "latest":
        out_dir = out_root / "latest"
        ensure_dir(out_dir)
        out_dirs.append(str(out_dir))
        for name, sql, params in datasets:
            _export_query(conn, sql, params, out_dir / f"{name}.csv")
            files_written += 1
    else:
        for name, sql, params in datasets:
            d = out_root / "history" / name
            ensure_dir(d)
            if str(d) not in out_dirs:
                out_dirs.append(str(d))
            _export_query(conn, sql, params, d / f"{ts}_{name}.csv")
            files_written += 1

    conn.close()
    return PublishResult(files_written=files_written, output_dirs=out_dirs)
