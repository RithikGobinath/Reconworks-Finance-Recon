from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

import shutil
import sqlite3

from .config import ProjectConfig
from .util import ensure_dir, utc_now_iso

@dataclass(frozen=True)
class PQPublishResult:
    batch_id: str
    drop_dir: Path
    files_written: int

DEFAULT_DATASETS = (
    "qa_flags.csv",
    "matches.csv",
    "unmatched_transactions.csv",
    "unmatched_vendor_payments.csv",
    "exceptions.csv",
    "rpt_spend_by_month_vendor.csv",
    "rpt_match_rate_by_month.csv",
    "rpt_exceptions_by_code.csv",
    "rpt_top_vendors.csv",
)

def _latest_batch_id(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT batch_id FROM ingest_files ORDER BY ingested_at_utc DESC LIMIT 1").fetchone()
    return row[0] if row else None

def publish_powerquery_drop(
    repo_root: Path,
    cfg: ProjectConfig,
    batch_id: Optional[str] = None,
    datasets: Iterable[str] = DEFAULT_DATASETS,
    drop_root: str = "out/pq_drop",
    mode: str = "history",
) -> PQPublishResult:
    """
    Create a Power Query-friendly folder drop.

    mode:
      - "latest": writes stable filenames under out/pq_drop/latest/
      - "history": writes versioned snapshots under out/pq_drop/history/<dataset_stem>/...
    """
    out_dir = repo_root / cfg.output_dir
    csv_dir = out_dir / "csv"
    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV dir not found: {csv_dir}. Run stages with --export-csv first.")

    # discover batch_id
    db_path = repo_root / cfg.database_path
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}. Run the pipeline first.")
    conn = sqlite3.connect(db_path)
    b = batch_id or _latest_batch_id(conn)
    conn.close()
    if not b:
        raise RuntimeError("No batch_id found in ingest_files.")

    root = repo_root / drop_root
    ts = utc_now_iso().replace(":", "").replace("+", "Z").replace("-", "")
    files_written = 0

    if mode not in ("latest", "history"):
        raise ValueError("mode must be 'latest' or 'history'")

    if mode == "latest":
        target_dir = root / "latest"
        ensure_dir(target_dir)
        for fname in datasets:
            src = csv_dir / fname
            if not src.exists():
                continue
            shutil.copy2(src, target_dir / fname)
            files_written += 1
        return PQPublishResult(batch_id=b, drop_dir=target_dir, files_written=files_written)

    # history mode
    # Create one folder per dataset, each containing versioned snapshots
    for fname in datasets:
        src = csv_dir / fname
        if not src.exists():
            continue
        stem = Path(fname).stem
        target_dir = root / "history" / stem
        ensure_dir(target_dir)
        # versioned file name includes timestamp and batch id for traceability
        out_name = f"{stem}__{ts}__{b}.csv"
        shutil.copy2(src, target_dir / out_name)
        files_written += 1

    return PQPublishResult(batch_id=b, drop_dir=(root / "history"), files_written=files_written)
