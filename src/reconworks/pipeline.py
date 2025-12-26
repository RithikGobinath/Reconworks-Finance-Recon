from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .config import load_config
from .ingest import ingest_all
from .mapping import map_all
from .cleaning import clean_all
from .normalization import normalize_all

def run_ingest(repo_root: Path, config_path: Path, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return ingest_all(repo_root=repo_root, cfg=cfg, export_csv=export_csv)

def run_mapping(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return map_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

def run_cleaning(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return clean_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

def run_normalize(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return normalize_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

from .modeling import model_all
from .qa_stage import qa_all

def run_model(repo_root: Path, config_path: Path, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return model_all(repo_root=repo_root, cfg=cfg, export_csv=export_csv)


def run_qa(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return qa_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)
