from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from src.pipeline.prediction_auto_prepare import (
    PredictionEnsurePolicy,
    ensure_predictions_for_date,
)


class FakeRepository:
    prediction_run: dict[str, Any] | None = None

    def __init__(self, _db_path: Path | str, _read_only: bool = False) -> None:
        pass

    def get_predictions_for_date(self, target_date: date):
        if self.prediction_run:
            payload = dict(self.prediction_run)
            payload["target_date"] = target_date.isoformat()
            return payload
        return None


class FakePipeline:
    instances: list["FakePipeline"] = []

    def __init__(self, _db_path: Path | str) -> None:
        self.calls: list[str] = []
        self.repository = FakeRepository(_db_path)
        self.repository.prediction_run = None
        FakePipeline.instances.append(self)

    def run_fetch(self, **_kwargs):
        self.calls.append("fetch")
        print("crawler noise should be hidden")
        return {"success": True, "summary": {"days_processed": 1}}

    def predict_for_date(self, target_date: date, **_kwargs):
        self.calls.append("predict")
        print("predict noise should be hidden")
        self.repository.prediction_run = {
            "id": "run-1",
            "target_date": target_date.isoformat(),
            "races": [],
        }
        return {"success": True, "prediction_run_id": "run-1"}


def test_ensure_predictions_returns_existing_without_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.duckdb"
    db_path.write_text("", encoding="utf-8")
    FakeRepository.prediction_run = {"id": "existing-run", "races": []}
    FakePipeline.instances.clear()

    result = ensure_predictions_for_date(
        target_date=date(2026, 4, 26),
        db_path=db_path,
        policy=PredictionEnsurePolicy(today=date(2026, 4, 26)),
        pipeline_factory=FakePipeline,
        repository_factory=FakeRepository,
    )

    assert result["success"] is True
    assert result["prepared"] is False
    assert result["prediction_run"]["id"] == "existing-run"
    assert FakePipeline.instances == []


def test_ensure_predictions_generates_today_when_missing(tmp_path: Path, capsys) -> None:
    FakeRepository.prediction_run = None
    FakePipeline.instances.clear()

    result = ensure_predictions_for_date(
        target_date=date(2026, 4, 26),
        db_path=tmp_path / "pipeline.duckdb",
        policy=PredictionEnsurePolicy(today=date(2026, 4, 26)),
        pipeline_factory=FakePipeline,
        repository_factory=FakeRepository,
    )

    assert result["success"] is True
    assert result["prepared"] is True
    assert result["prediction_run"]["id"] == "run-1"
    assert FakePipeline.instances[-1].calls == ["fetch", "predict"]
    assert capsys.readouterr().out == ""


def test_ensure_predictions_refuses_unapproved_date(tmp_path: Path) -> None:
    FakeRepository.prediction_run = None
    FakePipeline.instances.clear()

    result = ensure_predictions_for_date(
        target_date=date(2026, 4, 28),
        db_path=tmp_path / "pipeline.duckdb",
        policy=PredictionEnsurePolicy(today=date(2026, 4, 26)),
        pipeline_factory=FakePipeline,
        repository_factory=FakeRepository,
    )

    assert result["success"] is False
    assert result["reason"] == "auto_prepare_date_not_allowed"
    assert FakePipeline.instances == []
