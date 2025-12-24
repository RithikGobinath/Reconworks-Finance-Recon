from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from . import io as rw_io
from .config import ProjectConfig
from .db import connect, table_exists, add_columns_text, create_ingest_files_table, insert_ingest_file
from .util import utc_now_iso, sanitize_columns, stable_row_hash, ensure_dir

META_COLS = [
    "batch_id",
    "ingested_at_utc",
    "source_name",
    "source_file",
    "source_row_number",
    "row_hash",
]

def _glob_files(repo_root: Path, pattern: str) -> List[Path]:
    return sorted((repo_root / pattern).parent.glob((repo_root / pattern).name))

def ingest_all(repo_root: Path, cfg: ProjectConfig, export_csv: bool = False) -> Dict[str, int]:
    """Ingest all configured sources into SQLite staging tables."""
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    db_path = repo_root / cfg.database_path
    conn = connect(db_path)
    create_ingest_files_table(conn)

    batch_id = str(uuid.uuid4())
    ingested_at = utc_now_iso()

    summary: Dict[str, int] = {}
    for source_name, source_cfg in cfg.sources.items():
        pattern = source_cfg.path
        files = _glob_files(repo_root, pattern)
        if not files:
            summary[source_name] = 0
            continue

        table = f"stg_{source_name}_raw"
        total_rows = 0

        # We'll collect last batch for optional CSV export
        last_batch_frames: List[pd.DataFrame] = []

        for f in files:
            df = rw_io.read_table(f)
            original_cols = [str(c) for c in df.columns]
            sanitized_cols, col_map = sanitize_columns(original_cols)
            df.columns = sanitized_cols

            # Add meta columns
            df.insert(0, "batch_id", batch_id)
            df.insert(1, "ingested_at_utc", ingested_at)
            df.insert(2, "source_name", source_name)
            df.insert(3, "source_file", str(f.relative_to(repo_root)))
            df.insert(4, "source_row_number", range(1, len(df) + 1))

            # Create stable row_hash from ORIGINAL column names + values
            # so hashing is independent of our sanitization.
            # We build per-row dict using original headers.
            raw_values = df.drop(columns=META_COLS[:-1], errors="ignore")  # exclude hash itself
            # But raw_values currently has sanitized columns; we need mapping back to originals.
            # We'll invert col_map {orig->san} to {san->orig}.
            inv = {v: k for k, v in col_map.items()}
            def _hash_row(row: pd.Series) -> str:
                # Build dict with original col names
                d = {inv.get(k, k): row.get(k) for k in raw_values.columns}
                return stable_row_hash(d)

            df["row_hash"] = raw_values.apply(_hash_row, axis=1)

            # Ensure table schema can accept all columns
            if table_exists(conn, table):
                add_columns_text(conn, table, df.columns)
            # Write to SQLite
            df.to_sql(table, conn, if_exists="append", index=False)

            # Insert file registry row
            stat = f.stat()
            insert_ingest_file(conn, {
                "batch_id": batch_id,
                "source_name": source_name,
                "source_file": str(f.relative_to(repo_root)),
                "file_modified_at": utc_now_iso(),  # keeping simple; can use stat.mtime later
                "file_size_bytes": int(stat.st_size),
                "row_count": int(len(df)),
                "original_columns_json": json.dumps(original_cols, ensure_ascii=False),
                "sanitized_columns_json": json.dumps(sanitized_cols, ensure_ascii=False),
                "ingested_at_utc": ingested_at,
            })

            total_rows += len(df)
            last_batch_frames.append(df)

        summary[source_name] = total_rows

        if export_csv and last_batch_frames:
            export_df = pd.concat(last_batch_frames, ignore_index=True)
            out_csv = out_dir / "csv" / f"{table}.csv"
            export_df.to_csv(out_csv, index=False)

    conn.close()
    return summary
