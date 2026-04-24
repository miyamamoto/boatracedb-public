#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.local_prediction_service import LocalPredictionPipeline


def parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local BoatRace fetch/train/predict pipeline backed by DuckDB"
    )
    parser.add_argument(
        "--db-path",
        default="data/boatrace_pipeline.duckdb",
        help="DuckDB registry path",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch or import race data into DuckDB")
    fetch_parser.add_argument("--start-date", required=True, type=parse_date)
    fetch_parser.add_argument("--end-date", required=True, type=parse_date)
    fetch_parser.add_argument("--cache-dir", default="data/comprehensive_cache")
    fetch_parser.add_argument("--download-missing", action="store_true")
    fetch_parser.add_argument("--dry-run", action="store_true")

    train_parser = subparsers.add_parser("train", help="Train a local model and register it")
    train_parser.add_argument("--training-start-date", required=True, type=parse_date)
    train_parser.add_argument("--training-end-date", required=True, type=parse_date)
    train_parser.add_argument("--model-type", default="lightgbm")
    train_parser.add_argument("--no-activate", action="store_true")

    predict_parser = subparsers.add_parser("predict", help="Run predictions and export snapshots")
    predict_parser.add_argument("--target-date", required=True, type=parse_date)
    predict_parser.add_argument("--model-path")
    predict_parser.add_argument("--limit", type=int)

    export_parser = subparsers.add_parser("export", help="Re-export a prediction run snapshot")
    export_parser.add_argument("--prediction-run-id", required=True)

    subparsers.add_parser("status", help="Show pipeline registry status")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    pipeline = LocalPredictionPipeline(db_path=args.db_path)

    if args.command == "fetch":
        result = pipeline.run_fetch(
            start_date=args.start_date,
            end_date=args.end_date,
            cache_dir=args.cache_dir,
            download_missing=args.download_missing,
            dry_run=args.dry_run,
        )
    elif args.command == "train":
        result = pipeline.train_model(
            training_start_date=args.training_start_date,
            training_end_date=args.training_end_date,
            model_type=args.model_type,
            activate=not args.no_activate,
        )
    elif args.command == "predict":
        result = pipeline.predict_for_date(
            target_date=args.target_date,
            model_path=args.model_path,
            limit=args.limit,
        )
    elif args.command == "export":
        result = pipeline.export_prediction_run(args.prediction_run_id)
    else:
        result = pipeline.get_status()

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
