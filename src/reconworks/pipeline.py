from __future__ import annotations

from pathlib import Path
from typing import Dict

from .config import load_config
from .ingest import ingest_all

def run_ingest(repo_root: Path, config_path: Path, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return ingest_all(repo_root=repo_root, cfg=cfg, export_csv=export_csv)
