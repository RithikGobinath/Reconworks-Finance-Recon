from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .config import load_config
from .ingest import ingest_all
from .mapping import map_all
from .cleaning import clean_all
from .normalization import normalize_all
from .modeling import model_all
from .qa_stage import qa_all
from .matching import match_all
from .exceptions import exceptions_all
from .reporting import reports_all
from .excel_dashboard import build_excel

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

def run_model(repo_root: Path, config_path: Path, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return model_all(repo_root=repo_root, cfg=cfg, export_csv=export_csv)

def run_qa(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return qa_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

def run_match(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return match_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

def run_exceptions(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return exceptions_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

def run_reports(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    cfg = load_config(config_path)
    return reports_all(repo_root=repo_root, cfg=cfg, batch_id=batch_id, export_csv=export_csv)

def run_excel(repo_root: Path, config_path: Path, batch_id: Optional[str] = None) -> Dict[str, str]:
    cfg = load_config(config_path)
    return build_excel(repo_root=repo_root, cfg=cfg, batch_id=batch_id)

def run_postmodel(repo_root: Path, config_path: Path, batch_id: Optional[str] = None, export_csv: bool = False) -> Dict[str, int]:
    """Convenience runner for stages 6-9 (QA->match->exceptions->reports)."""
    # QA
    qa = run_qa(repo_root, config_path, batch_id=batch_id, export_csv=export_csv)
    # Match
    m = run_match(repo_root, config_path, batch_id=batch_id, export_csv=export_csv)
    # Exceptions (needs matches + qa flags)
    e = run_exceptions(repo_root, config_path, batch_id=batch_id, export_csv=export_csv)
    # Reports
    r = run_reports(repo_root, config_path, batch_id=batch_id, export_csv=export_csv)
    return {**{f"qa_{k}": v for k,v in qa.items()}, **{f"match_{k}": v for k,v in m.items()}, **e, **r}

from .powerquery_publish import publish_powerquery_drop

def run_publish_pq(repo_root: Path, config_path: Path, batch_id: Optional[str] = None) -> Dict[str, int]:
    cfg = load_config(config_path)
    res = publish_powerquery_drop(
        repo_root=repo_root,
        cfg=cfg,
        batch_id=batch_id,
        drop_root=cfg.powerquery.drop_root,
        mode=cfg.powerquery.mode,
    )
    return {"pq_files_written": res.files_written}
