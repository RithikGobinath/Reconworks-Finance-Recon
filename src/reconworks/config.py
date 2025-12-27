from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

@dataclass(frozen=True)
class SourceConfig:
    name: str
    path: str

@dataclass(frozen=True)
class ReferenceConfig:
    vendor_aliases_path: str
    policy_rules_path: str

@dataclass(frozen=True)
class MatchingConfig:
    date_window_days: int = 3
    amount_tolerance_cents: int = 0
    min_score: float = 0.80
    low_confidence_threshold: float = 0.90

@dataclass(frozen=True)
class PowerQueryConfig:
    drop_root: str = "out/pq_drop"
    mode: str = "history"  # "latest" or "history"

@dataclass(frozen=True)
class ProjectConfig:
    name: str
    output_dir: str
    database_path: str
    sources: Dict[str, SourceConfig]
    reference: ReferenceConfig
    matching: MatchingConfig
    powerquery: PowerQueryConfig

def load_config(config_path: str | Path) -> ProjectConfig:
    p = Path(config_path)
    data = tomllib.loads(p.read_text(encoding="utf-8"))

    project = data.get("project", {})
    sources_raw = data.get("sources", {})
    reference_raw = data.get("reference", {})
    matching_raw = data.get("matching", {})
    pq_raw = data.get("powerquery", {})

    sources: Dict[str, SourceConfig] = {}
    for key, val in sources_raw.items():
        sources[key] = SourceConfig(name=key, path=str(val["path"]))

    ref = ReferenceConfig(
        vendor_aliases_path=str(reference_raw.get("vendor_aliases_path", "data/reference/vendor_aliases.csv")),
        policy_rules_path=str(reference_raw.get("policy_rules_path", "data/reference/policy_rules.csv")),
    )

    matching = MatchingConfig(
        date_window_days=int(matching_raw.get("date_window_days", 3)),
        amount_tolerance_cents=int(matching_raw.get("amount_tolerance_cents", 0)),
        min_score=float(matching_raw.get("min_score", 0.80)),
        low_confidence_threshold=float(matching_raw.get("low_confidence_threshold", 0.90)),
    )

    powerquery = PowerQueryConfig(
        drop_root=str(pq_raw.get("drop_root", "out/pq_drop")),
        mode=str(pq_raw.get("mode", "history")),
    )

    return ProjectConfig(
        name=str(project.get("name", "ReconWorks")),
        output_dir=str(project.get("output_dir", "out")),
        database_path=str(project.get("database_path", "out/sqlite/reconworks.db")),
        sources=sources,
        reference=ref,
        matching=matching,
        powerquery=powerquery,
    )
