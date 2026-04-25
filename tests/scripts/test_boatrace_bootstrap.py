from __future__ import annotations

from datetime import date
from pathlib import Path

from scripts.boatrace_bootstrap import BootstrapConfig, BootstrapRunner


class DummyPipeline:
    def __init__(self, status_payload=None) -> None:
        self.calls = []
        self.status_payload = status_payload or {
            "db_path": "data/test.duckdb",
            "models": 0,
            "prediction_runs": 0,
            "active_model": None,
        }

    def run_fetch(self, **kwargs):
        self.calls.append(("fetch", kwargs))
        progress_callback = kwargs.get("progress_callback")
        if progress_callback is not None:
            progress_callback(
                "fetch:start",
                {
                    "start_date": kwargs["start_date"].isoformat(),
                    "end_date": kwargs["end_date"].isoformat(),
                    "total_days": 3,
                    "download_missing": kwargs["download_missing"],
                },
            )
            progress_callback(
                "fetch:complete",
                {"success": True, "summary": {"days_processed": 3, "days_missing": 0}},
            )
        return {
            "success": True,
            "summary": {
                "days_processed": 3,
                "days_missing": 0,
                "races_prerace": 36,
                "race_entries_prerace": 216,
                "race_results": 24,
                "odds_data": 12,
            },
        }

    def train_model(self, **kwargs):
        self.calls.append(("train", kwargs))
        progress_callback = kwargs.get("progress_callback")
        if progress_callback is not None:
            progress_callback("train:load_data", {})
            progress_callback("train:complete", {"model_path": "models/dummy.pkl"})
        return {
            "success": True,
            "model_path": "models/dummy.pkl",
            "feature_count": 36,
            "training_samples": 100,
            "validation_samples": 20,
            "validation_scores": {"logloss": 0.5},
        }

    def predict_for_date(self, **kwargs):
        self.calls.append(("predict", kwargs))
        progress_callback = kwargs.get("progress_callback")
        if progress_callback is not None:
            progress_callback("predict:load_model", {"target_date": kwargs["target_date"].isoformat()})
            progress_callback(
                "predict:complete",
                {
                    "prediction_run_id": "run-1",
                    "total_races": 12,
                    "output_paths": {"markdown": "output/predictions/latest.md"},
                },
            )
        return {
            "success": True,
            "prediction_run_id": "run-1",
            "target_date": kwargs["target_date"].isoformat(),
            "output_paths": {"markdown": "output/predictions/latest.md"},
        }

    def get_status(self):
        return self.status_payload


def test_bootstrap_config_derives_expected_windows() -> None:
    config = BootstrapConfig(target_date=date(2026, 4, 23), training_days=90, analysis_days=90)

    assert config.fetch_start_date == date(2026, 1, 23)
    assert config.fetch_end_date == date(2026, 4, 23)
    assert config.training_start_date == date(2026, 1, 23)
    assert config.training_end_date == date(2026, 4, 22)


def test_install_skills_copies_codex_and_claude_assets(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = BootstrapConfig(
        target_date=date(2026, 4, 23),
        training_days=7,
        codex_home=tmp_path / ".codex",
        claude_home=tmp_path / ".claude",
        summary_dir=str(tmp_path / "summaries"),
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=DummyPipeline())

    result = runner.install_skills()

    assert result["success"] is True
    assert (tmp_path / ".codex" / "skills" / "boatrace-predictions" / "SKILL.md").exists()
    assert (tmp_path / ".codex" / "skills" / "boatrace-program-sheet" / "SKILL.md").exists()
    assert (tmp_path / ".claude" / "skills" / "boatrace-predictions" / "SKILL.md").exists()
    assert (tmp_path / ".claude" / "skills" / "boatrace-program-sheet" / "SKILL.md").exists()
    assert (tmp_path / ".claude" / "agents" / "boatrace-predictions.md").exists()
    assert (tmp_path / ".claude" / "agents" / "boatrace-program-sheet.md").exists()


def test_bootstrap_runner_executes_pipeline_and_writes_summary(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    dummy_pipeline = DummyPipeline()
    config = BootstrapConfig(
        target_date=date(2026, 4, 23),
        training_days=7,
        codex_home=tmp_path / ".codex",
        claude_home=tmp_path / ".claude",
        summary_dir=str(tmp_path / "summaries"),
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=dummy_pipeline)
    events = []

    summary = runner.run(progress_callback=lambda event, payload: events.append((event, payload)))

    call_names = [name for name, _kwargs in dummy_pipeline.calls]
    assert summary["success"] is True
    assert call_names == ["fetch", "train", "predict"]
    assert summary["summary_paths"]["json"].endswith("bootstrap-summary.json")
    assert Path(summary["summary_paths"]["json"]).exists()
    assert any(event == "bootstrap:complete" for event, _payload in events)


def test_bootstrap_runner_skips_train_when_model_is_recent(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    dummy_pipeline = DummyPipeline(
        status_payload={
            "db_path": "data/test.duckdb",
            "models": 1,
            "prediction_runs": 3,
            "active_model": {
                "id": "model-1",
                "training_start_date": "2026-01-24",
                "training_end_date": "2026-04-23",
                "created_at": "2026-04-23 10:00:00",
            },
        }
    )
    config = BootstrapConfig(
        target_date=date(2026, 4, 24),
        training_days=90,
        retrain_interval_days=7,
        codex_home=tmp_path / ".codex",
        claude_home=tmp_path / ".claude",
        summary_dir=str(tmp_path / "summaries"),
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=dummy_pipeline)

    summary = runner.run()

    call_names = [name for name, _kwargs in dummy_pipeline.calls]
    assert summary["train"]["skipped"] is True
    assert "model_recent_enough" == summary["train"]["reason"]
    assert call_names == ["fetch", "predict"]


def test_bootstrap_runner_retrains_when_model_is_stale(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    dummy_pipeline = DummyPipeline(
        status_payload={
            "db_path": "data/test.duckdb",
            "models": 1,
            "prediction_runs": 3,
            "active_model": {
                "id": "model-1",
                "training_start_date": "2026-01-17",
                "training_end_date": "2026-04-16",
                "created_at": "2026-04-16 10:00:00",
            },
        }
    )
    config = BootstrapConfig(
        target_date=date(2026, 4, 24),
        training_days=90,
        retrain_interval_days=7,
        codex_home=tmp_path / ".codex",
        claude_home=tmp_path / ".claude",
        summary_dir=str(tmp_path / "summaries"),
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=dummy_pipeline)

    decision = runner.decide_training_action()
    summary = runner.run()

    call_names = [name for name, _kwargs in dummy_pipeline.calls]
    assert decision["should_train"] is True
    assert "model_stale" in decision["reasons"]
    assert summary["train"]["success"] is True
    assert call_names == ["fetch", "train", "predict"]
