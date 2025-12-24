from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def stable_row_hash(row: Dict[str, Any]) -> str:
    """Hash a row dict in a stable way (sorted keys)."""
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    return sha256_text(payload)

def sanitize_column(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"c_{s}"
    return s

def sanitize_columns(cols: Iterable[str]) -> Tuple[List[str], Dict[str, str]]:
    """Return a unique, safe column list and mapping {original -> sanitized}."""
    mapping: Dict[str, str] = {}
    used = {}
    sanitized_cols: List[str] = []
    for c in cols:
        base = sanitize_column(str(c))
        candidate = base
        i = 2
        while candidate in used:
            candidate = f"{base}_{i}"
            i += 1
        used[candidate] = True
        mapping[str(c)] = candidate
        sanitized_cols.append(candidate)
    return sanitized_cols, mapping

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
