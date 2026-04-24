from datetime import date, datetime
from types import SimpleNamespace

from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository


def build_prediction() -> SimpleNamespace:
    return SimpleNamespace(
        race_id=101,
        race_date=date(2026, 4, 24),
        venue_code="07",
        race_number=12,
        racer_predictions={
            1: {"1位": 0.42, "2位": 0.20, "3位": 0.10},
            2: {"1位": 0.21, "2位": 0.30, "3位": 0.18},
            3: {"1位": 0.14, "2位": 0.18, "3位": 0.22},
        },
        ticket_probabilities={
            "win": {"1": 0.42, "2": 0.21},
            "exacta": {"1-2": 0.11, "1-3": 0.09},
        },
        confidence_score=0.73,
        prediction_timestamp=datetime(2026, 4, 23, 12, 0, 0),
    )


def test_repository_round_trip(tmp_path):
    repository = DuckDBPredictionRepository(tmp_path / "pipeline.duckdb")

    model_id = repository.register_model(
        model_type="lightgbm",
        model_path="models/test_model.pkl",
        training_start_date=date(2025, 1, 1),
        training_end_date=date(2025, 12, 31),
        feature_count=128,
        training_samples=2048,
        validation_scores={"accuracy": 0.61},
        metadata={"source": "test"},
        activate=True,
    )
    assert repository.get_active_model()["id"] == model_id

    prediction_run_id = repository.start_prediction_run(
        target_date=date(2026, 4, 24),
        model_id=model_id,
        model_path="models/test_model.pkl",
        requested_races=[101],
    )
    summary = repository.save_prediction_run_results(
        prediction_run_id=prediction_run_id,
        predictions=[build_prediction()],
        output_path="output/predictions/2026-04-24/latest.json",
    )

    assert summary["total_races"] == 1
    assert summary["ticket_predictions"] == 4

    latest_run = repository.get_latest_prediction_run()
    assert latest_run["id"] == prediction_run_id
    assert latest_run["target_date"] == date(2026, 4, 24)

    run_details = repository.get_prediction_run_details(prediction_run_id)
    assert run_details is not None
    assert len(run_details["races"]) == 1
    assert run_details["races"][0]["venue_name"] == "蒲郡"

    race = repository.get_race_prediction(
        target_date=date(2026, 4, 24),
        venue_code="07",
        race_number=12,
    )
    assert race is not None
    assert race["top3"][0]["racer_id"] == 1
    assert race["ticket_predictions"]["win"][0]["combination"] == "1"


def test_fetch_run_lifecycle(tmp_path):
    repository = DuckDBPredictionRepository(tmp_path / "pipeline.duckdb")
    fetch_run_id = repository.start_fetch_run(
        source="data_download_concurrent.py",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 2),
        command=["python3", "scripts/data_download_concurrent.py"],
        parameters={"parallel_workers": 8},
    )
    repository.finish_fetch_run(
        fetch_run_id,
        status="completed",
        artifacts=[{"kind": "stdout_tail", "content": "done"}],
        note="ok",
    )

    summary = repository.get_status_summary()
    assert summary["fetch_runs"] == 1
    assert summary["latest_fetch_run"]["id"] == fetch_run_id
    assert summary["latest_fetch_run"]["artifacts"][0]["kind"] == "stdout_tail"
