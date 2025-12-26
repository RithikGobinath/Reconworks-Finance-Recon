from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

@dataclass(frozen=True)
class MappingConfig:
    vendor_raw: List[str]
    date_raw: List[str]
    amount_raw: List[str]

@dataclass(frozen=True)
class SourceConfig:
    name: str
    path: str
    mapping: MappingConfig

@dataclass(frozen=True)
class ProjectConfig:
    name: str
    output_dir: str
    database_path: str
    sources: Dict[str, SourceConfig]
    vendor_aliases_path: str = "data/reference/vendor_aliases.csv"
    policy_rules_path: str = "data/reference/policy_rules.csv"

def _lower_list(xs: List[str]) -> List[str]:
    return [str(x).strip().lower() for x in xs if str(x).strip()]

def load_config(config_path: str | Path) -> ProjectConfig:
    p = Path(config_path)
    data = tomllib.loads(p.read_text(encoding="utf-8"))

    project = data.get("project", {})
    sources_raw = data.get("sources", {})
    reference = data.get("reference", {})

    sources: Dict[str, SourceConfig] = {}
    for key, val in sources_raw.items():
        mapping_raw = val.get("mapping", {})
        mapping = MappingConfig(
            vendor_raw=_lower_list(mapping_raw.get("vendor_raw", [])),
            date_raw=_lower_list(mapping_raw.get("date_raw", [])),
            amount_raw=_lower_list(mapping_raw.get("amount_raw", [])),
        )
        sources[key] = SourceConfig(
            name=key,
            path=str(val["path"]),
            mapping=mapping,
        )

    return ProjectConfig(
        name=str(project.get("name", "ReconWorks")),
        output_dir=str(project.get("output_dir", "out")),
        database_path=str(project.get("database_path", "out/sqlite/reconworks.db")),
        sources=sources,
        vendor_aliases_path=str(reference.get("vendor_aliases_path", "data/reference/vendor_aliases.csv")),
        policy_rules_path=str(reference.get("policy_rules_path", "data/reference/policy_rules.csv")),
    )
