from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from scripts.boatrace_bootstrap import BootstrapConfig, BootstrapRunner, build_config, build_parser


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


def _config_with_temp_integrations(tmp_path: Path, **kwargs) -> BootstrapConfig:
    defaults = {
        "codex_home": tmp_path / ".codex",
        "claude_home": tmp_path / ".claude",
        "claude_code_config_path": tmp_path / ".claude.json",
        "claude_desktop_config_path": tmp_path / "Claude" / "claude_desktop_config.json",
        "summary_dir": str(tmp_path / "summaries"),
    }
    defaults.update(kwargs)
    return BootstrapConfig(**defaults)


def test_bootstrap_config_derives_expected_windows() -> None:
    config = BootstrapConfig(target_date=date(2026, 4, 23), training_days=90, analysis_days=180)

    assert config.fetch_start_date == date(2025, 10, 25)
    assert config.fetch_end_date == date(2026, 4, 23)
    assert config.training_start_date == date(2026, 1, 23)
    assert config.training_end_date == date(2026, 4, 22)


def test_build_config_rejects_analysis_days_below_minimum() -> None:
    parser = build_parser()
    args = parser.parse_args(["--target-date", "2026-04-23", "--analysis-days", "179"])

    with pytest.raises(ValueError, match="analysis-days は 180 以上"):
        build_config(args)


def test_install_skills_copies_codex_and_claude_assets(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = _config_with_temp_integrations(
        tmp_path,
        target_date=date(2026, 4, 23),
        training_days=7,
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
    assert any(item["type"] == "claude_code_mcp" for item in result["installed"])
    assert any(item["type"] == "claude_desktop_mcp" for item in result["installed"])
    assert "boatrace-local" in (tmp_path / ".claude.json").read_text(encoding="utf-8")
    assert "boatrace-local" in (tmp_path / "Claude" / "claude_desktop_config.json").read_text(encoding="utf-8")


def test_install_mcp_preserves_existing_claude_config(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    claude_code_config_path = tmp_path / ".claude.json"
    claude_desktop_config_path = tmp_path / "Claude" / "claude_desktop_config.json"
    claude_code_config_path.write_text(
        '{"projects": {"/tmp/other": {"mcpServers": {"existing": {"command": "python"}}}}}',
        encoding="utf-8",
    )
    claude_desktop_config_path.parent.mkdir(parents=True, exist_ok=True)
    claude_desktop_config_path.write_text(
        '{"mcpServers": {"jvlink-remote": {"command": "ssh"}}}',
        encoding="utf-8",
    )
    config = _config_with_temp_integrations(
        tmp_path,
        target_date=date(2026, 4, 23),
        training_days=7,
        claude_code_config_path=claude_code_config_path,
        claude_desktop_config_path=claude_desktop_config_path,
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=DummyPipeline())

    runner.install_skills()

    code_payload = json.loads(claude_code_config_path.read_text(encoding="utf-8"))
    desktop_payload = json.loads(claude_desktop_config_path.read_text(encoding="utf-8"))
    assert "existing" in code_payload["projects"]["/tmp/other"]["mcpServers"]
    server = code_payload["projects"][str(project_root)]["mcpServers"]["boatrace-local"]
    assert server["type"] == "stdio"
    assert Path(server["env"]["BOATRACE_DB_PATH"]).name == "boatrace_pipeline.duckdb"
    assert Path(server["env"]["BOATRACE_DB_PATH"]).parent.name == "data"
    assert "jvlink-remote" in desktop_payload["mcpServers"]
    assert "boatrace-local" in desktop_payload["mcpServers"]


def test_mcp_config_uv_fallback_includes_runtime_dependencies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = Path(__file__).resolve().parents[2]
    config = _config_with_temp_integrations(
        tmp_path,
        target_date=date(2026, 4, 23),
        training_days=7,
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=DummyPipeline())
    monkeypatch.setattr(runner, "_mcp_python_path", lambda: None)
    monkeypatch.setattr(runner, "_local_uv_path", lambda: Path("/opt/homebrew/bin/uv"))

    server = runner.build_mcp_server_config()

    assert Path(server["command"]).name == "uv"
    assert server["args"][:3] == ["run", "--directory", str(project_root)]
    assert "duckdb>=1.0.0" in server["args"]
    assert "mcp>=1.9.0,<2" in server["args"]
    assert server["args"][-2] == "python"
    assert Path(server["args"][-1]).name == "boatrace_mcp_server.py"
    assert Path(server["args"][-1]).parent.name == "scripts"


def test_bootstrap_runner_executes_pipeline_and_writes_summary(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    dummy_pipeline = DummyPipeline()
    config = _config_with_temp_integrations(
        tmp_path,
        target_date=date(2026, 4, 23),
        training_days=7,
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
    config = _config_with_temp_integrations(
        tmp_path,
        target_date=date(2026, 4, 24),
        training_days=90,
        retrain_interval_days=7,
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
    config = _config_with_temp_integrations(
        tmp_path,
        target_date=date(2026, 4, 24),
        training_days=90,
        retrain_interval_days=7,
    )
    runner = BootstrapRunner(config=config, project_root=project_root, pipeline=dummy_pipeline)

    decision = runner.decide_training_action()
    summary = runner.run()

    call_names = [name for name, _kwargs in dummy_pipeline.calls]
    assert decision["should_train"] is True
    assert "model_stale" in decision["reasons"]
    assert summary["train"]["success"] is True
    assert call_names == ["fetch", "train", "predict"]
