#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository
from src.pipeline.prediction_auto_prepare import ensure_predictions_for_date


def parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query local BoatRace predictions from DuckDB")
    parser.add_argument("--db-path", default="data/boatrace_pipeline.duckdb")
    parser.add_argument("--cache-dir", default="data/comprehensive_cache")
    parser.add_argument("--download-missing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--auto-prepare",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Generate missing predictions before reading. Defaults to on for today/tomorrow commands.",
    )
    parser.add_argument("--force-prepare", action="store_true")
    parser.add_argument("--format", choices=["json", "markdown"], default="json")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status")
    subparsers.add_parser("latest")
    subparsers.add_parser("model")
    subparsers.add_parser("today")
    subparsers.add_parser("tomorrow")

    date_parser = subparsers.add_parser("date")
    date_parser.add_argument("--target-date", required=True, type=parse_date)

    race_parser = subparsers.add_parser("race")
    race_parser.add_argument("--target-date", required=True, type=parse_date)
    race_parser.add_argument("--venue-code", required=True)
    race_parser.add_argument("--race-number", required=True, type=int)
    return parser


def render_markdown_status(payload: Dict[str, Any]) -> str:
    source_tables = payload.get("source_tables") or {}
    source_date_range = payload.get("source_date_range") or {}
    lines = [
        "# BoatRace Pipeline Status",
        "",
        f"- DB: `{payload['db_path']}`",
        f"- Fetch Runs: {payload['fetch_runs']}",
        f"- Models: {payload['models']}",
        f"- Prediction Runs: {payload['prediction_runs']}",
        f"- Pre-race Races: {source_tables.get('races_prerace', 0)}",
        f"- Pre-race Entries: {source_tables.get('race_entries_prerace', 0)}",
        f"- Race Results: {source_tables.get('race_results', 0)}",
        f"- Source Odds: {source_tables.get('odds_data', 0)}",
    ]
    if source_date_range.get("days"):
        lines.extend(
            [
                f"- Source Date Range: {source_date_range.get('start_date')} -> {source_date_range.get('end_date')}",
                f"- Source Days: {source_date_range.get('days')}",
            ]
        )
    if payload.get("migration_required"):
        lines.extend(
            [
                "",
                "## Attention",
                "",
                f"- {payload.get('migration_note')}",
            ]
        )
    active_model = payload.get("active_model")
    if active_model:
        lines.extend(
            [
                "",
                "## Active Model",
                "",
                f"- ID: `{active_model['id']}`",
                f"- Type: `{active_model['model_type']}`",
                f"- Path: `{active_model['model_path']}`",
                f"- Training Window: {active_model.get('training_start_date')} -> {active_model.get('training_end_date')}",
            ]
        )
    return "\n".join(lines) + "\n"


def render_markdown_run(payload: Optional[Dict[str, Any]]) -> str:
    if not payload:
        return "# No prediction run found\n"

    lines = [
        f"# Predictions {payload['target_date']}",
        "",
        f"- Prediction Run: `{payload['id']}`",
        f"- Status: `{payload['status']}`",
        f"- Model Path: `{payload.get('model_path') or 'auto'}`",
        f"- Total Races: {payload.get('total_races', 0)}",
        "",
        "## Races",
        "",
    ]
    for race in payload.get("races", []):
        top3 = race.get("top3") or []
        top3_text = ", ".join(
            f"{item['racer_id']}号艇 {item['win_probability']:.1%}"
            for item in top3
        ) or "no ranking"
        lines.append(
            f"- {race.get('venue_name', race['venue_code'])} {race['race_number']}R"
            f" | confidence={float(race.get('confidence_score', 0.0)):.3f}"
            f" | {top3_text}"
        )
    return "\n".join(lines) + "\n"


def render_markdown_race(payload: Optional[Dict[str, Any]]) -> str:
    if not payload:
        return "# Race prediction not found\n"

    lines = [
        f"# {payload.get('venue_name', payload['venue_code'])} {payload['race_number']}R",
        "",
        f"- Date: {payload['target_date']}",
        f"- Prediction Run: `{payload['prediction_run_id']}`",
        f"- Confidence: {float(payload.get('confidence_score', 0.0)):.3f}",
        "",
        "## Top 3",
        "",
    ]
    for item in payload.get("top3") or []:
        lines.append(f"- {item['racer_id']}号艇: {item['win_probability']:.1%}")

    ticket_predictions = payload.get("ticket_predictions") or {}
    if ticket_predictions:
        lines.extend(["", "## Ticket Picks", ""])
        for ticket_type, combinations in ticket_predictions.items():
            lines.append(f"- {ticket_type}")
            for combination in combinations[:3]:
                lines.append(
                    f"  {combination['combination']} ({float(combination['probability']):.1%})"
                )

    return "\n".join(lines) + "\n"


def render_markdown_model(payload: Optional[Dict[str, Any]]) -> str:
    if not payload:
        return "# Active model not found\n"

    lines = [
        "# Active Model",
        "",
        f"- ID: `{payload['id']}`",
        f"- Type: `{payload['model_type']}`",
        f"- Path: `{payload['model_path']}`",
        f"- Created At: {payload.get('created_at')}",
    ]
    validation_scores = payload.get("validation_scores") or {}
    if validation_scores:
        lines.extend(["", "## Validation Scores", ""])
        for key, value in validation_scores.items():
            lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def render_output(command: str, payload: Any, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if command == "status":
        return render_markdown_status(payload)
    if command in {"latest", "date"}:
        return render_markdown_run(payload)
    if command == "race":
        return render_markdown_race(payload)
    if command == "model":
        return render_markdown_model(payload)
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def _resolve_target_date(command: str, args: argparse.Namespace) -> Optional[date]:
    if command == "today":
        return date.today()
    if command == "tomorrow":
        return date.today() + timedelta(days=1)
    if command in {"date", "race"}:
        return args.target_date
    return None


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    db_path = Path(args.db_path)
    target_date = _resolve_target_date(args.command, args)
    should_auto_prepare = (
        args.auto_prepare
        if args.auto_prepare is not None
        else args.command in {"today", "tomorrow"}
    )

    ensure_result = None
    if target_date is not None and should_auto_prepare:
        ensure_result = ensure_predictions_for_date(
            target_date=target_date,
            db_path=db_path,
            cache_dir=args.cache_dir,
            download_missing=args.download_missing,
            force=args.force_prepare,
        )
        if ensure_result.get("success"):
            payload = ensure_result.get("prediction_run")
            if args.command == "race":
                payload = None
                for race in (ensure_result.get("prediction_run") or {}).get("races", []):
                    if race["venue_code"] == args.venue_code and int(race["race_number"]) == int(args.race_number):
                        race["prediction_run_id"] = ensure_result["prediction_run"]["id"]
                        race["target_date"] = ensure_result["prediction_run"]["target_date"]
                        race["model_id"] = ensure_result["prediction_run"].get("model_id")
                        race["model_path"] = ensure_result["prediction_run"].get("model_path")
                        payload = race
                        break
            if args.format == "json":
                print(json.dumps({"ensure": ensure_result, "payload": payload}, ensure_ascii=False, indent=2, default=str))
            else:
                prefix = ""
                if ensure_result.get("prepared"):
                    prefix = f"対象日 {target_date.isoformat()} の予測を新しく生成しました。\n\n"
                print(prefix + render_output("race" if args.command == "race" else "date", payload, args.format))
            return 0
        if args.command in {"today", "tomorrow"}:
            if args.format == "json":
                print(json.dumps({"ensure": ensure_result, "payload": None}, ensure_ascii=False, indent=2, default=str))
            else:
                print(f"対象日 {target_date.isoformat()} の予測を準備できませんでした: {ensure_result.get('error')}")
            return 2

    if not db_path.exists():
        parser.error(
            f"DuckDB not found: {db_path}. Run boatrace-local-pipeline status or fetch first."
        )
    repository = DuckDBPredictionRepository(db_path, read_only=True)

    if args.command == "status":
        payload = repository.get_status_summary()
    elif args.command == "latest":
        latest = repository.get_latest_prediction_run()
        payload = repository.get_prediction_run_details(latest["id"]) if latest else None
    elif args.command == "model":
        payload = repository.get_active_model()
    elif args.command in {"date", "today", "tomorrow"}:
        payload = repository.get_predictions_for_date(target_date)
    else:
        payload = repository.get_race_prediction(
            target_date=target_date,
            venue_code=args.venue_code,
            race_number=args.race_number,
        )

    print(render_output("race" if args.command == "race" else ("date" if args.command in {"today", "tomorrow"} else args.command), payload, args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
