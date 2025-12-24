from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

@dataclass(frozen=True)
class SourceConfig:
    name: str
    path: str

@dataclass(frozen=True)
class ProjectConfig:
    name: str
    output_dir: str
    database_path: str
    sources: Dict[str, SourceConfig]

def load_config(config_path: str | Path) -> ProjectConfig:
    p = Path(config_path)
    data = tomllib.loads(p.read_text(encoding="utf-8"))
    project = data.get("project", {})
    sources_raw = data.get("sources", {})

    sources: Dict[str, SourceConfig] = {}
    for key, val in sources_raw.items():
        sources[key] = SourceConfig(name=key, path=str(val["path"]))

    return ProjectConfig(
        name=str(project.get("name", "ReconWorks")),
        output_dir=str(project.get("output_dir", "out")),
        database_path=str(project.get("database_path", "out/sqlite/reconworks.db")),
        sources=sources,
    )
