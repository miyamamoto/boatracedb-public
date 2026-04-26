from __future__ import annotations

import json
import os
import pickle
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import pandas as pd

from .duckdb_prediction_repository import (
    DEFAULT_PIPELINE_DB_PATH,
    DuckDBPredictionRepository,
    VENUE_CODE_TO_NAME,
)
from .prediction_commentary import attach_prediction_commentary, render_run_commentary_markdown
from .prediction_disclaimer import render_prediction_disclaimer_markdown

TICKET_TYPE_LABELS = {
    "win": "単勝",
    "exacta": "2連単",
    "quinella": "2連複",
    "trifecta": "3連単",
    "trio": "3連複",
}

if TYPE_CHECKING:
    from src.crawler.comprehensive_parser import ComprehensiveBoatRaceParser


class LocalPredictionPipeline:
    """DuckDB-native fetch, train, predict, and export pipeline."""

    def __init__(self, db_path: Path | str = DEFAULT_PIPELINE_DB_PATH):
        self.project_root = Path(__file__).resolve().parents[2]
        self.repository = DuckDBPredictionRepository(db_path)

    @staticmethod
    def _emit_progress(
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]],
        event: str,
        **payload: Any,
    ) -> None:
        if progress_callback is not None:
            progress_callback(event, payload)

    def run_fetch(
        self,
        start_date: date,
        end_date: date,
        cache_dir: str = "data/comprehensive_cache",
        download_missing: bool = False,
        dry_run: bool = False,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        command = [
            sys.executable,
            "scripts/boatrace_local_pipeline.py",
            "fetch",
            "--start-date",
            start_date.isoformat(),
            "--end-date",
            end_date.isoformat(),
            "--cache-dir",
            cache_dir,
        ]
        if download_missing:
            command.append("--download-missing")

        fetch_run_id = self.repository.start_fetch_run(
            source="duckdb_native_fetch",
            start_date=start_date,
            end_date=end_date,
            command=command,
            parameters={
                "cache_dir": cache_dir,
                "download_missing": download_missing,
                "dry_run": dry_run,
            },
            note="DuckDB native cache-aware fetch",
            status="planned" if dry_run else "running",
        )

        if dry_run:
            self.repository.finish_fetch_run(
                fetch_run_id,
                status="planned",
                artifacts=[],
                note="dry-run only; fetch was not executed",
            )
            return {
                "success": True,
                "fetch_run_id": fetch_run_id,
                "dry_run": True,
                "command": command,
            }

        from src.crawler.comprehensive_parser import ComprehensiveBoatRaceParser

        parser = ComprehensiveBoatRaceParser()
        crawler: Optional[Any] = None
        cache_path = self.project_root / cache_dir
        artifacts: List[Dict[str, Any]] = []
        totals = {
            "days_processed": 0,
            "days_missing": 0,
            "races_prerace": 0,
            "race_entries_prerace": 0,
            "race_results": 0,
            "odds_data": 0,
        }
        total_days = max((end_date - start_date).days + 1, 0)
        self._emit_progress(
            progress_callback,
            "fetch:start",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            total_days=total_days,
            download_missing=download_missing,
        )

        for current_day, target_date in enumerate(self._date_range(start_date, end_date), start=1):
            self._emit_progress(
                progress_callback,
                "fetch:day_started",
                current=current_day,
                total=total_days,
                target_date=target_date.isoformat(),
            )
            schedule_files = self._load_raw_files(cache_path, target_date, "schedule")
            performance_files = self._load_raw_files(cache_path, target_date, "performance")

            if download_missing and (not schedule_files or not performance_files):
                self._emit_progress(
                    progress_callback,
                    "fetch:downloading_missing",
                    current=current_day,
                    total=total_days,
                    target_date=target_date.isoformat(),
                    missing_schedule=not schedule_files,
                    missing_performance=not performance_files,
                )
                if crawler is None:
                    from src.crawler.lzh_crawler import LzhCrawler

                    crawler = LzhCrawler(cache_dir=str(cache_path))
                if not schedule_files:
                    schedule_files = crawler.download_race_data(target_date, "schedule")
                if not performance_files:
                    performance_files = crawler.download_race_data(target_date, "performance")

            if not schedule_files and not performance_files:
                totals["days_missing"] += 1
                artifacts.append(
                    {
                        "date": target_date.isoformat(),
                        "status": "missing",
                        "message": "cache/download data not found",
                    }
                )
                self._emit_progress(
                    progress_callback,
                    "fetch:day_completed",
                    current=current_day,
                    total=total_days,
                    target_date=target_date.isoformat(),
                    status="missing",
                )
                continue

            schedule_parsed = self._parse_schedule(parser, schedule_files, target_date)
            performance_parsed = self._parse_performance(parser, performance_files)
            counts = self.repository.replace_source_snapshot(
                target_date=target_date,
                schedule_data=schedule_parsed,
                performance_data=performance_parsed,
            )
            totals["days_processed"] += 1
            totals["races_prerace"] += counts.get("races_prerace", 0)
            totals["race_entries_prerace"] += counts.get("race_entries_prerace", 0)
            totals["race_results"] += counts.get("race_results", 0)
            totals["odds_data"] += counts["odds_data"]
            artifacts.append(
                {
                    "date": target_date.isoformat(),
                    "status": "loaded",
                    "counts": counts,
                }
            )
            self._emit_progress(
                progress_callback,
                "fetch:day_completed",
                current=current_day,
                total=total_days,
                target_date=target_date.isoformat(),
                status="loaded",
                counts=counts,
            )

        success = totals["days_processed"] > 0
        self.repository.finish_fetch_run(
            fetch_run_id,
            status="completed" if success else "failed",
            artifacts=artifacts[-20:],
            note=f"processed={totals['days_processed']} missing={totals['days_missing']}",
        )
        self._emit_progress(
            progress_callback,
            "fetch:complete",
            success=success,
            summary=totals,
        )
        return {
            "success": success,
            "fetch_run_id": fetch_run_id,
            "command": command,
            "summary": totals,
            "artifacts": artifacts[-10:],
        }

    def train_model(
        self,
        training_start_date: date,
        training_end_date: date,
        model_type: str = "lightgbm",
        activate: bool = True,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        if model_type != "lightgbm":
            raise ValueError("DuckDB ローカル学習は lightgbm のみ対応しています")

        self._ensure_mplconfigdir()
        from .local_duckdb_modeling import train_local_model

        self._emit_progress(
            progress_callback,
            "train:load_data",
            training_start_date=training_start_date.isoformat(),
            training_end_date=training_end_date.isoformat(),
        )
        entries_df = self._load_dataframe(
            """
            SELECT
                entries.*,
                results.result_position,
                results.result_time,
                results.disqualified
            FROM race_entries_prerace AS entries
            INNER JOIN race_results AS results
                ON entries.race_date = results.race_date
               AND entries.venue_code = results.venue_code
               AND entries.race_number = results.race_number
               AND entries.boat_number = results.boat_number
            WHERE entries.race_date >= ? AND entries.race_date <= ?
            ORDER BY entries.race_date, entries.venue_code, entries.race_number, entries.boat_number
            """,
            [training_start_date, training_end_date],
        )
        races_df = self._load_dataframe(
            """
            SELECT *
            FROM races_prerace
            WHERE race_date >= ? AND race_date <= ?
            ORDER BY race_date, venue_code, race_number
            """,
            [training_start_date, training_end_date],
        )
        if entries_df.empty or races_df.empty:
            raise ValueError("学習に必要な DuckDB データがありません。先に fetch を実行してください")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_output_path = self.project_root / "models" / f"duckdb_local_model_{timestamp}.pkl"
        result = train_local_model(
            entries_df=entries_df,
            races_df=races_df,
            model_output_path=model_output_path,
            training_start_date=training_start_date,
            training_end_date=training_end_date,
            progress_callback=progress_callback,
        )
        self._emit_progress(progress_callback, "train:register_model", model_path=str(model_output_path))
        model_id = self.repository.register_model(
            model_type="duckdb_lightgbm_win",
            model_path=str(model_output_path),
            training_start_date=training_start_date,
            training_end_date=training_end_date,
            feature_count=len(result["feature_columns"]),
            training_samples=result["training_samples"],
            validation_scores=result["validation_scores"],
            metadata={
                "feature_importance": result["feature_importance"],
                "feature_audit": result.get("feature_audit", {}),
            },
            activate=activate,
        )
        self._emit_progress(
            progress_callback,
            "train:registered",
            model_id=model_id,
            model_path=str(model_output_path),
            activate=activate,
        )
        return {
            "success": True,
            "model_id": model_id,
            "model_type": "duckdb_lightgbm_win",
            "model_path": str(model_output_path),
            "feature_columns": result["feature_columns"],
            "feature_count": len(result["feature_columns"]),
            "training_samples": result["training_samples"],
            "validation_samples": result["validation_samples"],
            "validation_scores": result["validation_scores"],
            "feature_importance": result["feature_importance"],
            "feature_audit": result.get("feature_audit", {}),
        }

    def predict_for_date(
        self,
        target_date: date,
        model_path: Optional[str] = None,
        limit: Optional[int] = None,
        allow_in_sample: bool = False,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        self._emit_progress(progress_callback, "predict:load_model", target_date=target_date.isoformat())
        active_model = self.repository.get_active_model()
        resolved_model_path = model_path or (active_model["model_path"] if active_model else None)
        if not resolved_model_path:
            raise ValueError("予測に使うモデルがありません。先に train を実行してください")

        self._ensure_mplconfigdir()
        from .local_duckdb_modeling import load_model_bundle, predict_local_races

        model_bundle = load_model_bundle(resolved_model_path)
        model_training_end_date = self._parse_optional_date(
            (active_model or {}).get("training_end_date")
        ) or self._parse_optional_date(model_bundle.get("training_end_date"))
        if (
            not allow_in_sample
            and model_training_end_date is not None
            and target_date <= model_training_end_date
        ):
            raise ValueError(
                "予測対象日がモデルの学習終了日以前です。"
                " retrospective な検証以外では in-sample 予測を許可しません。"
            )
        resolved_model_id = (
            active_model["id"]
            if active_model and active_model["model_path"] == resolved_model_path
            else None
        )
        if not resolved_model_id:
            resolved_model_id = self.repository.register_model(
                model_type=model_bundle.get("model_type", "duckdb_lightgbm_win"),
                model_path=resolved_model_path,
                training_start_date=self._parse_optional_date(model_bundle.get("training_start_date")),
                training_end_date=self._parse_optional_date(model_bundle.get("training_end_date")),
                feature_count=len(model_bundle.get("feature_columns", [])),
                training_samples=0,
                validation_scores=model_bundle.get("validation_scores", {}),
                metadata={
                    "feature_importance": model_bundle.get("feature_importance", []),
                    "feature_audit": model_bundle.get("feature_audit", {}),
                },
                activate=True,
            )

        target_races_df = self._load_dataframe(
            """
            SELECT *
            FROM races_prerace
            WHERE race_date = ?
            ORDER BY venue_code, race_number
            """,
            [target_date],
        )
        if target_races_df.empty:
            raise ValueError(f"対象日のレースが DuckDB にありません: {target_date}")
        if limit is not None:
            target_races_df = target_races_df.head(limit)
        self._emit_progress(
            progress_callback,
            "predict:load_target_races",
            target_date=target_date.isoformat(),
            races=int(len(target_races_df)),
        )

        target_entries_df = self._load_dataframe(
            """
            SELECT *
            FROM race_entries_prerace
            WHERE race_date = ?
            ORDER BY venue_code, race_number, boat_number
            """,
            [target_date],
        )
        target_entries_df = target_entries_df.merge(
            target_races_df[["race_date", "venue_code", "race_number"]],
            on=["race_date", "venue_code", "race_number"],
            how="inner",
        )
        if target_entries_df.empty:
            raise ValueError(f"対象日の出走表が DuckDB にありません: {target_date}")
        self._emit_progress(
            progress_callback,
            "predict:load_history",
            target_date=target_date.isoformat(),
            target_entries=int(len(target_entries_df)),
        )

        history_entries_df = self._load_dataframe(
            """
            SELECT
                entries.*,
                results.result_position,
                results.result_time,
                results.disqualified
            FROM race_entries_prerace AS entries
            INNER JOIN race_results AS results
                ON entries.race_date = results.race_date
               AND entries.venue_code = results.venue_code
               AND entries.race_number = results.race_number
               AND entries.boat_number = results.boat_number
            WHERE entries.race_date < ?
            ORDER BY entries.race_date, entries.venue_code, entries.race_number, entries.boat_number
            """,
            [target_date],
        )
        history_races_df = self._load_dataframe(
            """
            SELECT *
            FROM races_prerace
            WHERE race_date < ?
            ORDER BY race_date, venue_code, race_number
            """,
            [target_date],
        )

        requested_races = self._build_race_ids(target_races_df)
        prediction_run_id = self.repository.start_prediction_run(
            target_date=target_date,
            model_id=resolved_model_id,
            model_path=resolved_model_path,
            requested_races=requested_races,
            note="duckdb local prediction pipeline",
        )
        self._emit_progress(
            progress_callback,
            "predict:run_started",
            prediction_run_id=prediction_run_id,
            requested_races=len(requested_races),
        )
        predictions = predict_local_races(
            model_bundle=model_bundle,
            history_entries_df=history_entries_df,
            history_races_df=history_races_df,
            target_entries_df=target_entries_df,
            target_races_df=target_races_df,
            progress_callback=progress_callback,
        )
        if not predictions:
            raise ValueError("予測対象レースの生成に失敗しました")

        self._emit_progress(progress_callback, "predict:exporting", prediction_run_id=prediction_run_id)
        exported_paths = self._export_prediction_snapshot(
            prediction_run_id=prediction_run_id,
            target_date=target_date,
            model_id=resolved_model_id,
            model_path=resolved_model_path,
        )
        summary = self.repository.save_prediction_run_results(
            prediction_run_id=prediction_run_id,
            predictions=predictions,
            output_path=exported_paths["json"],
            status="completed",
            note=f"model_path={resolved_model_path}",
        )
        details = self.repository.get_prediction_run_details(prediction_run_id)
        refreshed_paths = self._export_prediction_snapshot(
            prediction_run_id=prediction_run_id,
            target_date=target_date,
            model_id=resolved_model_id,
            model_path=resolved_model_path,
            details=details,
        )
        self._emit_progress(
            progress_callback,
            "predict:complete",
            prediction_run_id=prediction_run_id,
            total_races=summary.get("total_races", 0),
            output_paths=refreshed_paths,
        )

        return {
            "success": True,
            "prediction_run_id": prediction_run_id,
            "target_date": target_date.isoformat(),
            "model_id": resolved_model_id,
            "model_path": resolved_model_path,
            "requested_races": requested_races,
            "summary": summary,
            "output_paths": refreshed_paths,
        }

    def export_prediction_run(self, prediction_run_id: str) -> Dict[str, str]:
        details = self.repository.get_prediction_run_details(prediction_run_id)
        if not details:
            raise ValueError(f"prediction_run_id not found: {prediction_run_id}")
        target_date = details["target_date"]
        return self._export_prediction_snapshot(
            prediction_run_id=prediction_run_id,
            target_date=target_date,
            model_id=details.get("model_id"),
            model_path=details.get("model_path"),
            details=details,
        )

    def get_status(self) -> Dict[str, Any]:
        return self.repository.get_status_summary()

    def _load_raw_files(
        self,
        cache_dir: Path,
        target_date: date,
        data_type: str,
    ) -> Dict[str, str]:
        cache_file = (
            cache_dir
            / target_date.strftime("%Y")
            / target_date.strftime("%m")
            / f"lzh_{data_type}_{target_date.strftime('%Y%m%d')}.cache"
        )
        if not cache_file.exists():
            return {}
        with cache_file.open("rb") as handle:
            payload = pickle.load(handle)
        return payload.get("data", {})

    def _parse_schedule(
        self,
        parser: ComprehensiveBoatRaceParser,
        files: Dict[str, str],
        target_date: date,
    ) -> Dict[str, Any]:
        if not files:
            return {}
        combined = {
            "venues": [],
            "races": [],
            "race_entries": [],
            "racers": [],
            "schedule_info": [],
        }
        seen_venues: set[str] = set()

        for content in files.values():
            file_lines = str(content).splitlines()
            sections = self._split_schedule_sections(file_lines)
            if not sections:
                sections = [(None, file_lines)]

            for venue_code, section_lines in sections:
                parsed = parser.parse_schedule_file_comprehensive(section_lines)
                normalized = self._normalize_schedule_section(
                    parsed=parsed,
                    venue_code=venue_code,
                    target_date=target_date,
                )
                for venue in normalized.get("venues", []):
                    code = str(venue.get("code") or "")
                    if code and code not in seen_venues:
                        combined["venues"].append(venue)
                        seen_venues.add(code)
                combined["races"].extend(normalized.get("races", []))
                combined["race_entries"].extend(normalized.get("race_entries", []))
                combined["racers"].extend(normalized.get("racers", []))
                combined["schedule_info"].extend(normalized.get("schedule_info", []))

        return combined

    def _parse_performance(
        self,
        parser: ComprehensiveBoatRaceParser,
        files: Dict[str, str],
    ) -> Dict[str, Any]:
        if not files:
            return {}
        lines: List[str] = []
        for content in files.values():
            lines.extend(str(content).splitlines())
        return parser.parse_performance_file_comprehensive(lines)

    def _split_schedule_sections(self, lines: List[str]) -> List[tuple[Optional[str], List[str]]]:
        sections: List[tuple[Optional[str], List[str]]] = []
        current_venue_code: Optional[str] = None
        current_lines: List[str] = []

        for line in lines:
            stripped = line.strip()
            if len(stripped) == 6 and stripped.endswith("BBGN") and stripped[:2].isdigit():
                if current_venue_code is not None and current_lines:
                    sections.append((current_venue_code, current_lines))
                current_venue_code = stripped[:2]
                current_lines = ["STARTB", line]
                continue
            current_lines.append(line)

        if current_venue_code is not None and current_lines:
            sections.append((current_venue_code, current_lines))
        return sections

    def _normalize_schedule_section(
        self,
        parsed: Dict[str, Any],
        venue_code: Optional[str],
        target_date: date,
    ) -> Dict[str, Any]:
        normalized = {
            "venues": [],
            "races": [],
            "race_entries": [],
            "racers": parsed.get("racers", []),
            "schedule_info": parsed.get("schedule_info", []),
        }

        resolved_venue_code = venue_code or self._extract_first_schedule_venue_code(parsed)
        resolved_venue_name = (
            VENUE_CODE_TO_NAME.get(resolved_venue_code, resolved_venue_code)
            if resolved_venue_code
            else None
        )

        if resolved_venue_code and resolved_venue_name:
            normalized["venues"].append({"code": resolved_venue_code, "name": resolved_venue_name})

        for race in parsed.get("races", []):
            row = dict(race)
            if resolved_venue_code:
                row["venue_code"] = resolved_venue_code
            row["race_date"] = target_date
            normalized["races"].append(row)

        for entry in parsed.get("race_entries", []):
            row = dict(entry)
            if resolved_venue_code:
                row["venue_code"] = resolved_venue_code
            row["race_date"] = target_date
            normalized["race_entries"].append(row)

        return normalized

    def _extract_first_schedule_venue_code(self, parsed: Dict[str, Any]) -> Optional[str]:
        for venue in parsed.get("venues", []):
            code = venue.get("code")
            if code:
                return str(code)
        for race in parsed.get("races", []):
            code = race.get("venue_code")
            if code:
                return str(code)
        return None

    def _load_dataframe(self, query: str, params: List[Any]) -> pd.DataFrame:
        with self.repository.connect() as conn:
            return conn.execute(query, params).fetchdf()

    def _build_race_ids(self, races_df: pd.DataFrame) -> List[int]:
        race_ids: List[int] = []
        for _, row in races_df.iterrows():
            race_date = pd.Timestamp(row["race_date"]).date()
            race_ids.append(
                int(
                    f"{race_date.strftime('%Y%m%d')}{int(row['venue_code']):02d}{int(row['race_number']):02d}"
                )
            )
        return race_ids

    def _parse_optional_date(self, value: Any) -> Optional[date]:
        if not value:
            return None
        return datetime.fromisoformat(str(value)).date()

    def _date_range(self, start_date: date, end_date: date) -> List[date]:
        current = start_date
        dates = []
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    def _ensure_mplconfigdir(self) -> None:
        mpl_dir = self.project_root / ".cache" / "matplotlib"
        mpl_dir.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("MPLCONFIGDIR", str(mpl_dir))

    def _export_prediction_snapshot(
        self,
        prediction_run_id: str,
        target_date: date,
        model_id: Optional[str],
        model_path: Optional[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        details = details or self.repository.get_prediction_run_details(prediction_run_id) or {}
        export_dir = self.project_root / "output" / "predictions" / target_date.isoformat()
        export_dir.mkdir(parents=True, exist_ok=True)

        snapshot = {
            "prediction_run_id": prediction_run_id,
            "target_date": target_date.isoformat(),
            "model_id": model_id,
            "model_path": model_path,
            "summary": details.get("summary", {}),
            "races": details.get("races", []),
        }
        attach_prediction_commentary(snapshot)

        json_path = export_dir / f"run_{prediction_run_id}.json"
        latest_json_path = export_dir / "latest.json"
        markdown_path = export_dir / f"run_{prediction_run_id}.md"
        latest_markdown_path = export_dir / "latest.md"

        json_text = json.dumps(snapshot, ensure_ascii=False, indent=2, default=str)
        markdown_text = self._render_markdown_snapshot(snapshot)

        json_path.write_text(json_text, encoding="utf-8")
        latest_json_path.write_text(json_text, encoding="utf-8")
        markdown_path.write_text(markdown_text, encoding="utf-8")
        latest_markdown_path.write_text(markdown_text, encoding="utf-8")

        return {
            "json": str(json_path),
            "json_latest": str(latest_json_path),
            "markdown": str(markdown_path),
            "markdown_latest": str(latest_markdown_path),
        }

    def _render_markdown_snapshot(self, snapshot: Dict[str, Any]) -> str:
        lines = [
            f"# BoatRace Predictions {snapshot['target_date']}",
            "",
            f"- Prediction Run: `{snapshot['prediction_run_id']}`",
            f"- Model ID: `{snapshot.get('model_id') or 'unknown'}`",
            f"- Model Path: `{snapshot.get('model_path') or 'auto'}`",
        ]

        summary = snapshot.get("summary") or {}
        if summary:
            lines.extend(
                [
                    f"- Races: {summary.get('total_races', 0)}",
                    f"- Ticket Predictions: {summary.get('ticket_predictions', 0)}",
                    "",
                ]
            )
        else:
            lines.append("")

        lines.extend([render_run_commentary_markdown(snapshot), ""])

        lines.append("## Race Predictions")
        lines.append("")

        for race in snapshot.get("races", []):
            top3 = race.get("top3") or []
            top3_text = ", ".join(
                f"{item['racer_id']}号艇 {item['win_probability']:.1%}"
                for item in top3
            ) or "no ranking"
            lines.append(
                f"- {race.get('venue_name', race.get('venue_code'))} {race['race_number']}R"
                f" | confidence={float(race.get('confidence_score', 0.0)):.3f}"
                f" | top3={top3_text}"
            )
            ticket_predictions = race.get("ticket_predictions") or {}
            for ticket_type, combinations in ticket_predictions.items():
                top_combination = combinations[0] if combinations else None
                if top_combination:
                    ticket_label = TICKET_TYPE_LABELS.get(ticket_type, ticket_type)
                    lines.append(
                        f"  {ticket_label}: {top_combination['combination']}"
                        f" ({float(top_combination['probability']):.1%})"
                    )

        lines.extend(["", render_prediction_disclaimer_markdown().rstrip()])
        return "\n".join(lines) + "\n"
