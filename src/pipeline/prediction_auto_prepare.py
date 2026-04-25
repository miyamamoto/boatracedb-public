from __future__ import annotations

import contextlib
import io
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .duckdb_prediction_repository import DuckDBPredictionRepository
from .local_prediction_service import LocalPredictionPipeline


DEFAULT_CACHE_DIR = "data/comprehensive_cache"


@dataclass(frozen=True)
class PredictionEnsurePolicy:
    today: date
    allow_tomorrow: bool = True

    def is_allowed(self, target_date: date) -> bool:
        allowed_dates = {self.today}
        if self.allow_tomorrow:
            allowed_dates.add(self.today + timedelta(days=1))
        return target_date in allowed_dates


def default_prediction_ensure_policy() -> PredictionEnsurePolicy:
    return PredictionEnsurePolicy(today=date.today(), allow_tomorrow=True)


def ensure_predictions_for_date(
    target_date: date,
    db_path: Path | str = "data/boatrace_pipeline.duckdb",
    cache_dir: str = DEFAULT_CACHE_DIR,
    download_missing: bool = True,
    force: bool = False,
    policy: Optional[PredictionEnsurePolicy] = None,
    progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    pipeline_factory: Callable[[Path | str], LocalPredictionPipeline] = LocalPredictionPipeline,
    repository_factory: Callable[[Path | str, bool], DuckDBPredictionRepository] = DuckDBPredictionRepository,
) -> Dict[str, Any]:
    """Ensure predictions exist for today/tomorrow, then return the prediction run.

    This intentionally does not train models. It only performs a controlled fetch
    and predict for dates allowed by policy, preventing arbitrary write operations
    from free-form user requests.
    """
    policy = policy or default_prediction_ensure_policy()
    db_path = Path(db_path)

    existing = None
    if db_path.exists():
        existing_repository = repository_factory(db_path, True)
        existing = existing_repository.get_predictions_for_date(target_date)
    if existing and not force:
        return {
            "success": True,
            "target_date": target_date.isoformat(),
            "prepared": False,
            "reason": "prediction_already_exists",
            "prediction_run": existing,
        }

    if not policy.is_allowed(target_date):
        return {
            "success": False,
            "target_date": target_date.isoformat(),
            "prepared": False,
            "reason": "auto_prepare_date_not_allowed",
            "error": "自動予測生成は今日または明日だけに制限されています。",
        }

    pipeline = pipeline_factory(db_path)
    captured_stdout = io.StringIO()
    captured_stderr = io.StringIO()
    with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
        fetch_result = pipeline.run_fetch(
            start_date=target_date,
            end_date=target_date,
            cache_dir=cache_dir,
            download_missing=download_missing,
            progress_callback=progress_callback,
        )
    if not fetch_result.get("success"):
        return {
            "success": False,
            "target_date": target_date.isoformat(),
            "prepared": False,
            "reason": "fetch_failed",
            "fetch": fetch_result,
            "error": "対象日の番組データを取得できませんでした。",
        }

    try:
        with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
            predict_result = pipeline.predict_for_date(
                target_date=target_date,
                progress_callback=progress_callback,
            )
    except Exception as exc:
        return {
            "success": False,
            "target_date": target_date.isoformat(),
            "prepared": False,
            "reason": "predict_failed",
            "fetch": fetch_result,
            "error": str(exc),
        }

    prediction_run = pipeline.repository.get_predictions_for_date(target_date)
    return {
        "success": True,
        "target_date": target_date.isoformat(),
        "prepared": True,
        "reason": "prediction_generated",
        "fetch": fetch_result,
        "predict": predict_result,
        "prediction_run": prediction_run,
    }
