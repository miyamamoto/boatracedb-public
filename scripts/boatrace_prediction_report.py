#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository
from src.pipeline.prediction_report import write_prediction_report


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate rich daily BoatRace prediction reports")
    parser.add_argument("--db-path", default="data/boatrace_pipeline.duckdb")
    parser.add_argument("--target-date", type=parse_date)
    parser.add_argument("--latest", action="store_true", help="Use the latest prediction run")
    parser.add_argument("--output-dir", default="output/prediction-reports")
    parser.add_argument("--no-pdf", action="store_true")
    parser.add_argument("--format", choices=["json", "markdown"], default="json")
    return parser


def _load_prediction_run(repository: DuckDBPredictionRepository, target_date: Optional[date], latest: bool) -> Dict[str, Any]:
    if latest:
        latest_run = repository.get_latest_prediction_run()
        if not latest_run:
            raise ValueError("予測結果が見つかりません。先に予測を生成してください。")
        run = repository.get_prediction_run_details(latest_run["id"])
    elif target_date:
        run = repository.get_predictions_for_date(target_date)
    else:
        raise ValueError("--target-date または --latest を指定してください。")

    if not run:
        label = "latest" if latest else target_date.isoformat() if target_date else "-"
        raise ValueError(f"対象の予測結果が見つかりません: {label}")
    return run


def _render_markdown(payload: Dict[str, Any]) -> str:
    lines = [
        "# Prediction Report Generated",
        "",
        f"- Target Date: {payload['target_date']}",
        f"- Races: {payload['race_count']}",
        f"- Venues: {payload['venue_count']}",
        f"- Markdown: `{payload['markdown_path']}`",
    ]
    if payload.get("pdf_path"):
        lines.append(f"- PDF: `{payload['pdf_path']}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.latest and args.target_date:
        parser.error("--latest と --target-date は同時に指定できません")

    try:
        repository = DuckDBPredictionRepository(args.db_path, read_only=True)
        prediction_run = _load_prediction_run(repository, args.target_date, args.latest)
        output = write_prediction_report(
            prediction_run,
            output_dir=args.output_dir,
            include_pdf=not args.no_pdf,
        )
        payload = {
            "success": True,
            "target_date": output.target_date.isoformat(),
            "race_count": output.race_count,
            "venue_count": output.venue_count,
            "markdown_path": str(output.markdown_path),
            "pdf_path": str(output.pdf_path) if output.pdf_path.exists() else None,
        }
    except Exception as exc:
        payload = {"success": False, "error": str(exc)}
        if args.format == "json":
            print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        else:
            print(f"# Prediction Report Failed\n\n- {exc}")
        return 1

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    else:
        print(_render_markdown(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
