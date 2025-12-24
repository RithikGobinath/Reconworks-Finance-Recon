from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import run_ingest
from .sample_data import write_sample_raw

def main() -> None:
    parser = argparse.ArgumentParser(prog="reconworks", description="ReconWorks pipeline (stage-by-stage).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-sample-data", help="Generate small sample raw CSV files under data/raw/")
    p_init.add_argument("--repo-root", default=".", help="Repo root path (default: current directory)")

    p_ingest = sub.add_parser("ingest", help="Stage 1: ingest raw inputs into SQLite staging tables")
    p_ingest.add_argument("--config", default="config.toml", help="Path to config.toml")
    p_ingest.add_argument("--repo-root", default=".", help="Repo root path (default: current directory)")
    p_ingest.add_argument("--export-csv", action="store_true", help="Export latest batch staging tables to out/csv/")

    args = parser.parse_args()
    repo_root = Path(args.repo_root).resolve()

    if args.cmd == "init-sample-data":
        write_sample_raw(repo_root)
        print(f"✅ Wrote sample raw data to: {repo_root / 'data' / 'raw'}")
        return

    if args.cmd == "ingest":
        summary = run_ingest(repo_root=repo_root, config_path=repo_root / args.config, export_csv=bool(args.export_csv))
        print("✅ Ingest complete.")
        for k, v in summary.items():
            print(f"  - {k}: {v} rows")
        print(f"DB: {(repo_root / 'out' / 'sqlite' / 'reconworks.db')}")
        return

if __name__ == "__main__":
    main()
