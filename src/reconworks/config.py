from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any, Optional

@dataclass(frozen=True)
class MappingConfig:
    vendor_raw: List[str]
    date_raw: List[str]
    amount_raw: List[str]

@dataclass(frozen=True)
class SourceConfig:
    name: str
    path: str
    mapping: Optional[MappingConfig] = None

@dataclass(frozen=True)
class ProjectConfig:
    name: str
    output_dir: str
    database_path: str
    sources: Dict[str, SourceConfig]

def _get_list(d: Dict[str, Any], key: str) -> List[str]:
    v = d.get(key, [])
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        return [v]
    return []

def load_config(config_path: str | Path) -> ProjectConfig:
    p = Path(config_path)
    data = tomllib.loads(p.read_text(encoding="utf-8"))
    project = data.get("project", {})
    sources_raw = data.get("sources", {})

    sources: Dict[str, SourceConfig] = {}
    for key, val in sources_raw.items():
        mapping_raw = val.get("mapping") if isinstance(val, dict) else None
        mapping = None
        if isinstance(mapping_raw, dict):
            mapping = MappingConfig(
                vendor_raw=_get_list(mapping_raw, "vendor_raw"),
                date_raw=_get_list(mapping_raw, "date_raw"),
                amount_raw=_get_list(mapping_raw, "amount_raw"),
            )
        sources[key] = SourceConfig(name=key, path=str(val["path"]), mapping=mapping)

    return ProjectConfig(
        name=str(project.get("name", "ReconWorks")),
        output_dir=str(project.get("output_dir", "out")),
        database_path=str(project.get("database_path", "out/sqlite/reconworks.db")),
        sources=sources,
    )
