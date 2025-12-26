from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import ProjectConfig
from .db import (
    connect,
    table_exists,
    add_columns_text,
    latest_batch_id,
    create_normalization_runs_table,
    insert_normalization_run,
)
from .util import utc_now_iso, ensure_dir

def _load_vendor_aliases(path: Path) -> List[Tuple[re.Pattern, str, str]]:
    """Load vendor alias patterns as case-insensitive regex rules.
    Returns list of (compiled_regex, canonical_vendor, pattern_str).
    """
    if not path.exists():
        return []
    rules: List[Tuple[re.Pattern, str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pat = (row.get("pattern") or "").strip()
            canon = (row.get("canonical_vendor") or "").strip()
            if not pat or not canon:
                continue
            try:
                rx = re.compile(pat, re.IGNORECASE)
            except re.error:
                continue
            rules.append((rx, canon, pat))
    return rules

_NOISE_TOKENS = {
    "inc","llc","ltd","co","corp","company","the",
    "pos","debit","credit","purchase","online",
    "com","help","mktp","us","store","payment"
}

def vendor_clean_text(v: str) -> str:
    """Normalize a messy vendor string into a matchable token string."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    # Drop trailing codes after '*'
    if "*" in s:
        s = s.split("*", 1)[0]
    # Remove store ids like '#04921'
    s = re.sub(r"#\s*\d+", " ", s)
    # Replace punctuation with spaces
    s = re.sub(r"[^A-Za-z0-9]+", " ", s)
    # Remove standalone digit groups
    s = re.sub(r"\b\d+\b", " ", s)
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)

    tokens = [t for t in s.split() if t and t not in _NOISE_TOKENS]
    return " ".join(tokens)

def canonicalize_vendor(vendor_raw: str, rules: List[Tuple[re.Pattern, str, str]]) -> Tuple[Optional[str], str, float, str]:
    """Return (vendor_canonical, method, confidence, notes)."""
    if vendor_raw is None or str(vendor_raw).strip() == "":
        return None, "missing", 0.0, "vendor_raw empty"

    raw = str(vendor_raw)
    clean = vendor_clean_text(raw)

    for rx, canon, pat in rules:
        if rx.search(raw) or (clean and rx.search(clean)):
            return canon, "alias_regex", 0.95, f"matched pattern: {pat}"

    if clean:
        return clean.title(), "clean_fallback", 0.60, "no alias match; used cleaned vendor"

    return None, "missing", 0.0, "vendor_clean empty"

def _normalize_one_source(
    repo_root: Path,
    cfg: ProjectConfig,
    source_name: str,
    batch_id: str,
    export_csv: bool,
) -> int:
    out_dir = repo_root / cfg.output_dir
    ensure_dir(out_dir / "csv")
    ensure_dir(out_dir / "sqlite")

    db_path = repo_root / cfg.database_path
    conn = connect(db_path)
    create_normalization_runs_table(conn)

    input_table = f"clean_{source_name}"
    output_table = f"norm_{source_name}"

    if not table_exists(conn, input_table):
        conn.close()
        return 0

    # Idempotent per batch
    if table_exists(conn, output_table):
        conn.execute(f"DELETE FROM {output_table} WHERE batch_id = ?", (batch_id,))
        conn.commit()

    df = pd.read_sql_query(
        f"SELECT * FROM {input_table} WHERE batch_id = ?",
        conn,
        params=(batch_id,),
    )
    if df.empty:
        conn.close()
        return 0

    # Dedupe (safety)
    if "row_hash" in df.columns:
        df = df.drop_duplicates(subset=["batch_id", "row_hash"], keep="last")

    if "vendor_raw" not in df.columns:
        df["vendor_raw"] = ""

    alias_path = repo_root / cfg.vendor_aliases_path
    rules = _load_vendor_aliases(alias_path)

    df["vendor_clean"] = df["vendor_raw"].apply(vendor_clean_text)
    res = df["vendor_raw"].apply(lambda v: canonicalize_vendor(v, rules))
    df["vendor_canonical"] = res.apply(lambda t: t[0])
    df["vendor_norm_method"] = res.apply(lambda t: t[1])
    df["vendor_norm_confidence"] = res.apply(lambda t: t[2])
    df["vendor_norm_notes"] = res.apply(lambda t: t[3])
    df["normalized_at_utc"] = utc_now_iso()

    total = int(len(df))
    alias_matches = int((df["vendor_norm_method"] == "alias_regex").sum())

    if table_exists(conn, output_table):
        add_columns_text(conn, output_table, df.columns)
    df.to_sql(output_table, conn, if_exists="append", index=False)

    insert_normalization_run(conn, {
        "normalized_at_utc": utc_now_iso(),
        "batch_id": batch_id,
        "source_name": source_name,
        "input_table": input_table,
        "output_table": output_table,
        "alias_file": str(alias_path.relative_to(repo_root)) if alias_path.exists() else None,
        "row_count": total,
        "alias_match_count": alias_matches,
        "no_match_count": total - alias_matches,
    })

    if export_csv:
        out_csv = out_dir / "csv" / f"{output_table}.csv"
        df.to_csv(out_csv, index=False)

    conn.close()
    return total

def normalize_all(
    repo_root: Path,
    cfg: ProjectConfig,
    batch_id: Optional[str] = None,
    export_csv: bool = False,
) -> Dict[str, int]:
    db_path = repo_root / cfg.database_path
    conn = connect(db_path)
    b = batch_id or latest_batch_id(conn)
    conn.close()
    if not b:
        return {name: 0 for name in cfg.sources.keys()}

    summary: Dict[str, int] = {}
    for source_name in cfg.sources.keys():
        summary[source_name] = _normalize_one_source(
            repo_root=repo_root,
            cfg=cfg,
            source_name=source_name,
            batch_id=b,
            export_csv=export_csv,
        )
    return summary
