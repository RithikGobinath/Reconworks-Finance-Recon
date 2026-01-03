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
class MatchingConfig:
    date_window_days: int = 3
    amount_tolerance_cents: int = 0
    min_score: float = 0.85
    low_confidence_threshold: float = 0.90
    vendor_weight: float = 0.6
    date_weight: float = 0.3
    amount_weight: float = 0.1

@dataclass(frozen=True)
class ReportingConfig:
    top_n_vendors: int = 20

@dataclass(frozen=True)
class ExcelConfig:
    output_path: str = "out/excel/recon_dashboard.xlsx"

@dataclass(frozen=True)
class PowerQueryConfig:
    drop_root: str = "out/pq_drop"
    mode: str = "history"  # "history" or "latest"

@dataclass(frozen=True)
class ProjectConfig:
    name: str
    output_dir: str
    database_path: str
    sources: Dict[str, SourceConfig]
    vendor_aliases_path: str
    policy_rules_path: str
    matching: MatchingConfig
    reporting: ReportingConfig
    excel: ExcelConfig
    powerquery: PowerQueryConfig

def _lower_list(xs: List[str]) -> List[str]:
    return [str(x).strip() for x in xs if str(x).strip()]

def load_config(config_path: str | Path) -> ProjectConfig:
    p = Path(config_path)
    data = tomllib.loads(p.read_text(encoding="utf-8"))

    project = data.get("project", {})
    sources_raw = data.get("sources", {})
    reference = data.get("reference", {})
    matching_raw = data.get("matching", {})
    reporting_raw = data.get("reporting", {})
    excel_raw = data.get("excel", {})
    powerquery_raw = data.get("powerquery", {})

    sources: Dict[str, SourceConfig] = {}
    for key, val in sources_raw.items():
        mapping_raw = (val.get("mapping", {}) or {})
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

    matching = MatchingConfig(
        date_window_days=int(matching_raw.get("date_window_days", 3)),
        amount_tolerance_cents=int(matching_raw.get("amount_tolerance_cents", 0)),
        min_score=float(matching_raw.get("min_score", 0.85)),
        low_confidence_threshold=float(matching_raw.get("low_confidence_threshold", 0.90)),
        vendor_weight=float(matching_raw.get("vendor_weight", 0.6)),
        date_weight=float(matching_raw.get("date_weight", 0.3)),
        amount_weight=float(matching_raw.get("amount_weight", 0.1)),
    )

    reporting = ReportingConfig(
        top_n_vendors=int(reporting_raw.get("top_n_vendors", 20))
    )

    excel = ExcelConfig(
        output_path=str(excel_raw.get("output_path", "out/excel/recon_dashboard.xlsx"))
    )

    powerquery = PowerQueryConfig(
        drop_root=str(powerquery_raw.get("drop_root", "out/pq_drop")),
        mode=str(powerquery_raw.get("mode", "history")),
    )

    return ProjectConfig(
        name=str(project.get("name", "ReconWorks")),
        output_dir=str(project.get("output_dir", "out")),
        database_path=str(project.get("database_path", "out/sqlite/reconworks.db")),
        sources=sources,
        vendor_aliases_path=str(reference.get("vendor_aliases_path", "data/reference/vendor_aliases.csv")),
        policy_rules_path=str(reference.get("policy_rules_path", "data/reference/policy_rules.csv")),
        matching=matching,
        reporting=reporting,
        excel=excel,
        powerquery=powerquery,
    )
