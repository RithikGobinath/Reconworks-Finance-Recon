from __future__ import annotations

from pathlib import Path
from typing import Tuple
import pandas as pd

def read_table(path: Path) -> pd.DataFrame:
    """Read a CSV or XLSX file as raw strings (preserve raw values)."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str, keep_default_na=False, na_values=[])
    raise ValueError(f"Unsupported file type: {path}")
