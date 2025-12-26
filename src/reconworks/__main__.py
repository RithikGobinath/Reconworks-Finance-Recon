from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import run_ingest, run_mapping, run_cleaning, run_normalize, run_model, run_qa
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

    p_map = sub.add_parser("map", help="Stage 2: map raw columns into canonical fields")
    p_map.add_argument("--config", default="config.toml")
    p_map.add_argument("--repo-root", default=".")
    p_map.add_argument("--batch-id", default=None)
    p_map.add_argument("--export-csv", action="store_true")

    p_clean = sub.add_parser("clean", help="Stage 3: clean mapped data (parse date + amount)")
    p_clean.add_argument("--config", default="config.toml")
    p_clean.add_argument("--repo-root", default=".")
    p_clean.add_argument("--batch-id", default=None)
    p_clean.add_argument("--export-csv", action="store_true")

    p_norm = sub.add_parser("normalize", help="Stage 4: normalize vendors using alias rules")
    p_norm.add_argument("--config", default="config.toml")
    p_norm.add_argument("--repo-root", default=".")
    p_norm.add_argument("--batch-id", default=None)
    p_norm.add_argument("--export-csv", action="store_true")

    p_model = sub.add_parser("model", help="Stage 5: build dim_vendor + fact tables")
    p_model.add_argument("--config", default="config.toml")
    p_model.add_argument("--repo-root", default=".")
    p_model.add_argument("--export-csv", action="store_true")

    p_qa = sub.add_parser("qa", help="Stage 6: QA checks (flags + policy rules)")
    p_qa.add_argument("--config", default="config.toml")
    p_qa.add_argument("--repo-root", default=".")
    p_qa.add_argument("--batch-id", default=None)
    p_qa.add_argument("--export-csv", action="store_true")

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

    if args.cmd == "map":
        summary = run_mapping(repo_root=repo_root, config_path=repo_root / args.config, batch_id=args.batch_id, export_csv=bool(args.export_csv))
        print("✅ Mapping complete.")
        for k, v in summary.items():
            print(f"  - {k}: {v} rows mapped")
        return

    if args.cmd == "clean":
        summary = run_cleaning(repo_root=repo_root, config_path=repo_root / args.config, batch_id=args.batch_id, export_csv=bool(args.export_csv))
        print("✅ Cleaning complete.")
        for k, v in summary.items():
            print(f"  - {k}: {v} rows cleaned")
        return

    if args.cmd == "normalize":
        summary = run_normalize(repo_root=repo_root, config_path=repo_root / args.config, batch_id=args.batch_id, export_csv=bool(args.export_csv))
        print("✅ Normalization complete.")
        for k, v in summary.items():
            print(f"  - {k}: {v} rows normalized")
        return

    if args.cmd == "model":
        summary = run_model(repo_root=repo_root, config_path=repo_root / args.config, export_csv=bool(args.export_csv))
        print("✅ Modeling complete.")
        for k, v in summary.items():
            print(f"  - {k}: {v} rows modeled")
        return

    if args.cmd == "qa":
        summary = run_qa(repo_root=repo_root, config_path=repo_root / args.config, batch_id=args.batch_id, export_csv=bool(args.export_csv))
        print("✅ QA complete.")
        for k, v in summary.items():
            print(f"  - {k}: {v}")
        return

if __name__ == "__main__":
    main()
