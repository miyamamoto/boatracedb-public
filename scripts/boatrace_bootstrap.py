#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.local_prediction_service import LocalPredictionPipeline

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    from rich.table import Table
except ImportError:  # pragma: no cover
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Progress = None  # type: ignore[assignment]
    SpinnerColumn = None  # type: ignore[assignment]
    TaskProgressColumn = None  # type: ignore[assignment]
    TextColumn = None  # type: ignore[assignment]
    TimeElapsedColumn = None  # type: ignore[assignment]
    TimeRemainingColumn = None  # type: ignore[assignment]
    BarColumn = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]


DEFAULT_DB_PATH = "data/boatrace_pipeline.duckdb"
DEFAULT_CACHE_DIR = "data/comprehensive_cache"
DEFAULT_ANALYSIS_DAYS = 180
MCP_SERVER_NAME = "boatrace-local"


def _default_claude_desktop_config_path() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    if os.name == "nt":
        appdata = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
        return appdata / "Claude" / "claude_desktop_config.json"
    return Path.home() / ".config" / "Claude" / "claude_desktop_config.json"


@dataclass
class BootstrapConfig:
    db_path: str = DEFAULT_DB_PATH
    cache_dir: str = DEFAULT_CACHE_DIR
    target_date: date = field(default_factory=date.today)
    training_days: int = 90
    analysis_days: int = DEFAULT_ANALYSIS_DAYS
    retrain_interval_days: int = 7
    download_missing: bool = True
    install_codex_skills: bool = True
    install_claude_skills: bool = True
    install_claude_agents: bool = True
    install_claude_mcp: bool = True
    codex_home: Path = field(default_factory=lambda: Path.home() / ".codex")
    claude_home: Path = field(default_factory=lambda: Path.home() / ".claude")
    claude_code_config_path: Path = field(default_factory=lambda: Path.home() / ".claude.json")
    claude_desktop_config_path: Path = field(default_factory=_default_claude_desktop_config_path)
    skip_fetch: bool = False
    skip_train: bool = False
    skip_predict: bool = False
    skip_skill_install: bool = False
    summary_dir: str = "output/bootstrap"

    @property
    def training_start_date(self) -> date:
        return self.target_date - timedelta(days=self.training_days)

    @property
    def training_end_date(self) -> date:
        return self.target_date - timedelta(days=1)

    @property
    def fetch_start_date(self) -> date:
        return self.target_date - timedelta(days=max(self.training_days, self.analysis_days))

    @property
    def fetch_end_date(self) -> date:
        return self.target_date

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["target_date"] = self.target_date.isoformat()
        payload["training_start_date"] = self.training_start_date.isoformat()
        payload["training_end_date"] = self.training_end_date.isoformat()
        payload["analysis_days"] = self.analysis_days
        payload["fetch_start_date"] = self.fetch_start_date.isoformat()
        payload["fetch_end_date"] = self.fetch_end_date.isoformat()
        payload["retrain_interval_days"] = self.retrain_interval_days
        payload["codex_home"] = str(self.codex_home)
        payload["claude_home"] = str(self.claude_home)
        payload["claude_code_config_path"] = str(self.claude_code_config_path)
        payload["claude_desktop_config_path"] = str(self.claude_desktop_config_path)
        return payload


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_optional_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value)
    for parser in (date.fromisoformat, lambda raw: datetime.fromisoformat(raw).date()):
        try:
            return parser(text)
        except ValueError:
            continue
    return None


def _copy_file(source: Path, destination: Path) -> Dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return {
        "source": str(source),
        "destination": str(destination),
    }


def _read_json_object(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON 設定ファイルを読めません: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON 設定ファイルの root は object である必要があります: {path}")
    return payload


def _write_json_object(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


class BootstrapRunner:
    def __init__(
        self,
        config: BootstrapConfig,
        project_root: Optional[Path] = None,
        pipeline: Optional[LocalPredictionPipeline] = None,
    ) -> None:
        self.config = config
        self.project_root = project_root or Path(__file__).resolve().parent.parent
        self.pipeline = pipeline or LocalPredictionPipeline(db_path=config.db_path)

    def run(
        self,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        progress_callback = progress_callback or (lambda _event, _payload: None)
        progress_callback("bootstrap:start", {"config": self.config.to_dict()})

        summary: Dict[str, Any] = {
            "success": False,
            "config": self.config.to_dict(),
            "fetch": None,
            "train": None,
            "predict": None,
            "skills": None,
            "status": None,
        }

        if self.config.skip_fetch:
            progress_callback("bootstrap:stage_skipped", {"stage": "fetch", "reason": "skip_fetch"})
            summary["fetch"] = {"skipped": True}
        else:
            progress_callback("bootstrap:stage_started", {"stage": "fetch", "label": "データ取得"})
            summary["fetch"] = self.pipeline.run_fetch(
                start_date=self.config.fetch_start_date,
                end_date=self.config.fetch_end_date,
                cache_dir=self.config.cache_dir,
                download_missing=self.config.download_missing,
                progress_callback=progress_callback,
            )
            progress_callback(
                "bootstrap:stage_completed",
                {"stage": "fetch", "summary": summary["fetch"].get("summary", {})},
            )

        if self.config.skip_train:
            progress_callback("bootstrap:stage_skipped", {"stage": "train", "reason": "skip_train"})
            summary["train"] = {"skipped": True}
        else:
            train_decision = self.decide_training_action()
            progress_callback("bootstrap:train_decision", train_decision)
            if train_decision["should_train"]:
                progress_callback("bootstrap:stage_started", {"stage": "train", "label": "特徴量作成と学習"})
                summary["train"] = self.pipeline.train_model(
                    training_start_date=self.config.training_start_date,
                    training_end_date=self.config.training_end_date,
                    progress_callback=progress_callback,
                )
                summary["train"]["decision"] = train_decision
                progress_callback(
                    "bootstrap:stage_completed",
                    {
                        "stage": "train",
                        "summary": {
                            "model_path": summary["train"].get("model_path"),
                            "validation_scores": summary["train"].get("validation_scores", {}),
                        },
                    },
                )
            else:
                summary["train"] = {
                    "skipped": True,
                    "reason": train_decision["reason"],
                    "decision": train_decision,
                    "active_model": train_decision.get("active_model"),
                }
                progress_callback(
                    "bootstrap:stage_skipped",
                    {"stage": "train", "reason": train_decision["reason"]},
                )

        if self.config.skip_predict:
            progress_callback("bootstrap:stage_skipped", {"stage": "predict", "reason": "skip_predict"})
            summary["predict"] = {"skipped": True}
        else:
            progress_callback("bootstrap:stage_started", {"stage": "predict", "label": "本日の予測生成"})
            summary["predict"] = self.pipeline.predict_for_date(
                target_date=self.config.target_date,
                progress_callback=progress_callback,
            )
            progress_callback(
                "bootstrap:stage_completed",
                {
                    "stage": "predict",
                    "summary": {
                        "prediction_run_id": summary["predict"].get("prediction_run_id"),
                        "target_date": summary["predict"].get("target_date"),
                        "output_paths": summary["predict"].get("output_paths", {}),
                    },
                },
            )

        if self.config.skip_skill_install:
            progress_callback(
                "bootstrap:stage_skipped",
                {"stage": "skills", "reason": "skip_skill_install"},
            )
            summary["skills"] = {"skipped": True}
        else:
            progress_callback("bootstrap:stage_started", {"stage": "skills", "label": "skill と agent の導入"})
            summary["skills"] = self.install_skills(progress_callback=progress_callback)
            progress_callback(
                "bootstrap:stage_completed",
                {
                    "stage": "skills",
                    "summary": {"installed_items": len(summary["skills"].get("installed", []))},
                },
            )

        summary["status"] = self.pipeline.get_status()
        summary["success"] = True
        summary["summary_paths"] = self.write_summary(summary)
        progress_callback("bootstrap:complete", {"summary": summary})
        return summary

    def decide_training_action(self) -> Dict[str, Any]:
        status = self.pipeline.get_status() or {}
        active_model = status.get("active_model")
        if not active_model:
            return {
                "should_train": True,
                "reason": "active_model_missing",
                "message": "アクティブモデルがないため学習します。",
                "active_model": None,
            }

        training_start = parse_optional_date(active_model.get("training_start_date"))
        training_end = parse_optional_date(active_model.get("training_end_date"))
        created_at = parse_optional_date(active_model.get("created_at"))
        freshness_anchor = training_end or created_at
        window_days = (
            ((training_end - training_start).days + 1)
            if training_start is not None and training_end is not None
            else None
        )
        model_age_days = (
            max((self.config.target_date - freshness_anchor).days, 0)
            if freshness_anchor is not None
            else None
        )

        reasons: List[str] = []
        if window_days is None:
            reasons.append("model_window_unknown")
        elif window_days != self.config.training_days:
            reasons.append("training_window_mismatch")

        if model_age_days is None:
            reasons.append("model_freshness_unknown")
        elif model_age_days >= self.config.retrain_interval_days:
            reasons.append("model_stale")

        if reasons:
            return {
                "should_train": True,
                "reason": reasons[0],
                "reasons": reasons,
                "message": self._format_train_decision_message(
                    reasons=reasons,
                    model_age_days=model_age_days,
                    window_days=window_days,
                ),
                "active_model": active_model,
                "model_age_days": model_age_days,
                "window_days": window_days,
            }

        return {
            "should_train": False,
            "reason": "model_recent_enough",
            "message": (
                f"現在のアクティブモデルは {model_age_days} 日前まで学習済みなので、"
                f"{self.config.retrain_interval_days} 日間隔の再学習条件には達していません。"
            ),
            "active_model": active_model,
            "model_age_days": model_age_days,
            "window_days": window_days,
        }

    def _format_train_decision_message(
        self,
        reasons: List[str],
        model_age_days: Optional[int],
        window_days: Optional[int],
    ) -> str:
        messages: List[str] = []
        if "training_window_mismatch" in reasons:
            messages.append(
                f"現在の学習窓 {window_days} 日が設定値 {self.config.training_days} 日と一致しません。"
            )
        if "model_stale" in reasons and model_age_days is not None:
            messages.append(
                f"現在のモデルは {model_age_days} 日前までしか取り込んでおらず、"
                f"{self.config.retrain_interval_days} 日間隔を超えています。"
            )
        if "model_window_unknown" in reasons:
            messages.append("現在のモデルに学習窓情報がないため再学習します。")
        if "model_freshness_unknown" in reasons:
            messages.append("現在のモデルの鮮度が判定できないため再学習します。")
        return " ".join(messages)

    def _absolute_db_path(self) -> Path:
        db_path = Path(self.config.db_path)
        if db_path.is_absolute():
            return db_path
        return (self.project_root / db_path).resolve()

    def _local_uv_path(self) -> Optional[Path]:
        local_uv = self.project_root / ".tools" / "bin" / ("uv.exe" if os.name == "nt" else "uv")
        if local_uv.exists():
            return local_uv
        resolved = shutil.which("uv")
        return Path(resolved) if resolved else None

    def _mcp_python_path(self) -> Optional[Path]:
        candidates = [
            self.project_root / ".venv" / "bin" / "python",
            self.project_root / ".venv" / "Scripts" / "python.exe",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def build_mcp_server_config(self) -> Dict[str, Any]:
        python_path = self._mcp_python_path()
        if python_path is not None:
            command = str(python_path)
            args = [str(self.project_root / "scripts" / "boatrace_mcp_server.py")]
        else:
            uv_path = self._local_uv_path()
            if uv_path is not None:
                command = str(uv_path)
                args = [
                    "run",
                    "--directory",
                    str(self.project_root),
                    "--with-requirements",
                    str(self.project_root / "requirements.txt"),
                    "--with-editable",
                    str(self.project_root),
                    "--quiet",
                    "--no-progress",
                    "--no-dev",
                    "--no-default-groups",
                    "--no-project",
                ]
                args.extend(
                    [
                        "python",
                        str(self.project_root / "scripts" / "boatrace_mcp_server.py"),
                    ]
                )
            else:
                command = sys.executable
                args = [str(self.project_root / "scripts" / "boatrace_mcp_server.py")]

        return {
            "type": "stdio",
            "command": command,
            "args": args,
            "env": {
                "BOATRACE_PROJECT_ROOT": str(self.project_root),
                "BOATRACE_DB_PATH": str(self._absolute_db_path()),
            },
        }

    def install_claude_code_mcp_config(self) -> Dict[str, Any]:
        config_path = self.config.claude_code_config_path
        payload = _read_json_object(config_path)
        project_key = str(self.project_root)
        project_config = payload.setdefault("projects", {}).setdefault(project_key, {})
        project_config.setdefault("mcpServers", {})[MCP_SERVER_NAME] = self.build_mcp_server_config()
        _write_json_object(config_path, payload)
        return {
            "type": "claude_code_mcp",
            "destination": str(config_path),
            "server_name": MCP_SERVER_NAME,
            "project": project_key,
        }

    def install_claude_desktop_mcp_config(self) -> Dict[str, Any]:
        config_path = self.config.claude_desktop_config_path
        payload = _read_json_object(config_path)
        payload.setdefault("mcpServers", {})[MCP_SERVER_NAME] = self.build_mcp_server_config()
        _write_json_object(config_path, payload)
        return {
            "type": "claude_desktop_mcp",
            "destination": str(config_path),
            "server_name": MCP_SERVER_NAME,
        }

    def install_skills(
        self,
        progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        progress_callback = progress_callback or (lambda _event, _payload: None)
        installed: List[Dict[str, Any]] = []
        operations: List[tuple[str, Path, Path]] = []

        if self.config.install_codex_skills:
            for skill_name in ("boatrace-predictions", "boatrace-program-sheet"):
                source = self.project_root / "skills" / skill_name / "SKILL.md"
                destination = self.config.codex_home / "skills" / skill_name / "SKILL.md"
                operations.append(("codex_skill", source, destination))

        if self.config.install_claude_skills:
            for skill_name in ("boatrace-predictions", "boatrace-program-sheet"):
                source = self.project_root / "skills" / skill_name / "SKILL.md"
                destination = self.config.claude_home / "skills" / skill_name / "SKILL.md"
                operations.append(("claude_skill", source, destination))

        if self.config.install_claude_agents:
            for agent_name in ("boatrace-predictions.md", "boatrace-program-sheet.md"):
                source = self.project_root / ".claude" / "agents" / agent_name
                destination = self.config.claude_home / "agents" / agent_name
                operations.append(("claude_agent", source, destination))

        mcp_operations: List[str] = []
        if self.config.install_claude_mcp:
            mcp_operations.extend(["claude_code_mcp", "claude_desktop_mcp"])

        total = len(operations) + len(mcp_operations)
        progress_callback("skills:start", {"total": total})
        current = 0
        for item_type, source, destination in operations:
            current += 1
            progress_callback(
                "skills:item_started",
                {
                    "current": current,
                    "total": total,
                    "item_type": item_type,
                    "source": str(source),
                    "destination": str(destination),
                },
            )
            installed_item = _copy_file(source, destination)
            installed_item["type"] = item_type
            installed.append(installed_item)
            progress_callback(
                "skills:item_completed",
                {
                    "current": current,
                    "total": total,
                    "item_type": item_type,
                    "destination": str(destination),
                },
            )

        for item_type in mcp_operations:
            current += 1
            destination = (
                self.config.claude_code_config_path
                if item_type == "claude_code_mcp"
                else self.config.claude_desktop_config_path
            )
            progress_callback(
                "skills:item_started",
                {
                    "current": current,
                    "total": total,
                    "item_type": item_type,
                    "destination": str(destination),
                },
            )
            try:
                installed_item = (
                    self.install_claude_code_mcp_config()
                    if item_type == "claude_code_mcp"
                    else self.install_claude_desktop_mcp_config()
                )
            except (OSError, ValueError) as exc:
                installed_item = {
                    "type": item_type,
                    "destination": str(destination),
                    "success": False,
                    "error": str(exc),
                }
            installed.append(installed_item)
            progress_callback(
                "skills:item_completed",
                {
                    "current": current,
                    "total": total,
                    "item_type": item_type,
                    "destination": str(destination),
                },
            )

        return {
            "success": True,
            "installed": installed,
            "restart_notes": [
                "Restart Codex to pick up new skills.",
                "Restart Claude Code to pick up new skills, local agents, and the boatrace-local MCP server.",
                "Restart Claude Desktop or reconnect MCP servers to pick up boatrace-local.",
            ],
        }

    def write_summary(self, summary: Dict[str, Any]) -> Dict[str, str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = self.project_root / self.config.summary_dir / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)

        json_path = output_dir / "bootstrap-summary.json"
        markdown_path = output_dir / "bootstrap-summary.md"
        json_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        markdown_path.write_text(self.render_summary_markdown(summary), encoding="utf-8")
        return {
            "json": str(json_path),
            "markdown": str(markdown_path),
        }

    def render_summary_markdown(self, summary: Dict[str, Any]) -> str:
        lines = [
            "# BoatRace Bootstrap Summary",
            "",
            "BoatRace Local Predictor のセットアップは完了しました。",
            "",
            f"- Target Date: {self.config.target_date.isoformat()}",
            f"- Fetch Window: {self.config.fetch_start_date.isoformat()} -> {self.config.fetch_end_date.isoformat()}",
            f"- Training Window: {self.config.training_start_date.isoformat()} -> {self.config.training_end_date.isoformat()}",
            f"- Analysis History: {self.config.analysis_days} days",
            "",
        ]

        fetch_summary = (summary.get("fetch") or {}).get("summary", {})
        if fetch_summary:
            lines.extend(
                [
                    "## Fetch",
                    "",
                    f"- Days Processed: {fetch_summary.get('days_processed', 0)}",
                    f"- Days Missing: {fetch_summary.get('days_missing', 0)}",
                    f"- Races: {fetch_summary.get('races_prerace', 0)}",
                    f"- Entries: {fetch_summary.get('race_entries_prerace', 0)}",
                    f"- Results: {fetch_summary.get('race_results', 0)}",
                    "",
                ]
            )

        train_summary = summary.get("train") or {}
        if train_summary and not train_summary.get("skipped"):
            lines.extend(
                [
                    "## Train",
                    "",
                    f"- Model Path: {train_summary.get('model_path')}",
                    f"- Feature Count: {train_summary.get('feature_count')}",
                    f"- Training Samples: {train_summary.get('training_samples')}",
                    f"- Validation Samples: {train_summary.get('validation_samples')}",
                ]
            )
            for key, value in (train_summary.get("validation_scores") or {}).items():
                lines.append(f"- Validation {key}: {value}")
            lines.append("")
        elif train_summary and train_summary.get("skipped"):
            lines.extend(
                [
                    "## Train",
                    "",
                    f"- Skipped: {train_summary.get('reason')}",
                    f"- Note: {(train_summary.get('decision') or {}).get('message')}",
                    "",
                ]
            )

        predict_summary = summary.get("predict") or {}
        if predict_summary and not predict_summary.get("skipped"):
            lines.extend(
                [
                    "## Predict",
                    "",
                    f"- Target Date: {predict_summary.get('target_date')}",
                    f"- Prediction Run ID: {predict_summary.get('prediction_run_id')}",
                ]
            )
            for key, value in (predict_summary.get("output_paths") or {}).items():
                lines.append(f"- {key}: {value}")
            lines.append("")

        skill_summary = summary.get("skills") or {}
        if skill_summary and not skill_summary.get("skipped"):
            lines.extend(["## Skills / MCP", ""])
            for item in skill_summary.get("installed", []):
                lines.append(f"- {item['type']}: {item['destination']}")
            lines.append("")

        lines.extend(
            [
                "## Status",
                "",
                f"- DB Path: {(summary.get('status') or {}).get('db_path')}",
                f"- Models: {(summary.get('status') or {}).get('models')}",
                f"- Prediction Runs: {(summary.get('status') or {}).get('prediction_runs')}",
                "",
                "## Next Steps",
                "",
                "- Codex / Claude Code から新しい skill / MCP を認識させるには、必要に応じてアプリを再起動してください。",
                "- Claude Code / Claude Desktop では `boatrace-local` MCP server が自動登録されます。",
                "- 最新予測は `boatrace-prediction-query --format markdown latest` で確認できます。",
                "",
                "## Uninstall",
                "",
                "```bash",
                "rm -rf ~/boatracedb",
                "rm -rf ~/.codex/skills/boatrace-predictions ~/.codex/skills/boatrace-program-sheet",
                "rm -rf ~/.claude/skills/boatrace-predictions ~/.claude/skills/boatrace-program-sheet",
                "rm -f ~/.claude/agents/boatrace-predictions.md ~/.claude/agents/boatrace-program-sheet.md",
                "```",
                "",
                "`~/.claude.json` と Claude Desktop config に登録された `boatrace-local` MCP server は、",
                "Claude の MCP 設定画面または JSON から削除してください。",
                "",
            ]
        )
        return "\n".join(lines)


class RichBootstrapUI:
    STAGE_WEIGHTS = {
        "fetch": 35.0,
        "features": 30.0,
        "train": 20.0,
        "predict": 10.0,
        "skills": 5.0,
    }
    STAGE_ORDER = ["fetch", "features", "train", "predict", "skills"]

    def __init__(self) -> None:
        self.console = Console()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=24),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
        )
        self.overall_task_id: Optional[int] = None
        self.stage_ratios: Dict[str, float] = {stage: 0.0 for stage in self.STAGE_WEIGHTS}
        self.train_feature_total = 7
        self.predict_dynamic_total = 6

    def __enter__(self) -> "RichBootstrapUI":
        self.progress.start()
        self.overall_task_id = self.progress.add_task("準備中", total=100.0)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.progress.stop()

    def callback(self, event: str, payload: Dict[str, Any]) -> None:
        if event == "bootstrap:start":
            self.console.print(
                Panel.fit(
                    "\n".join(
                        [
                            "BoatRace ローカル bootstrap を開始します。",
                            f"対象日: {payload['config']['target_date']}",
                            f"学習期間: {payload['config']['training_start_date']} -> {payload['config']['training_end_date']}",
                            f"SQL分析用の履歴投入量: 直近 {payload['config']['analysis_days']} 日",
                            f"再学習間隔: {payload['config']['retrain_interval_days']} 日",
                            f"取得期間: {payload['config']['fetch_start_date']} -> {payload['config']['fetch_end_date']}",
                            "",
                            "初回はデータ取得、特徴量作成、LightGBM 学習に時間がかかります。",
                            "180日分ではデータ取得だけで約1時間、初回全体で1.5から2.5時間程度を見込んでください。",
                            "365日以上を選んだ場合は、数時間単位で長くなります。",
                            "画面下部には、現在実行中の1ステップだけを表示します。",
                            "特徴量作成では選手・モーター・会場などの過去成績を時系列で集計するため、ここが最も重い工程です。",
                        ]
                    ),
                    title="Bootstrap",
                )
            )
            return

        if event == "bootstrap:stage_started":
            stage = payload["stage"]
            label = payload.get("label", self._stage_label(stage))
            self.console.print(Panel.fit(self._stage_help(stage), title=f"{label} を開始"))
            return

        if event == "bootstrap:train_decision":
            message = payload.get("message", "")
            if payload.get("should_train"):
                self.console.print(f"[bold yellow]再学習します:[/bold yellow] {message}")
            else:
                self.console.print(f"[green]学習はスキップします:[/green] {message}")
            return

        if event == "bootstrap:stage_skipped":
            stage = payload["stage"]
            reason = payload.get("reason")
            suffix = f" skip: {reason}" if reason else " (skip)"
            if stage == "train":
                self._update_stage("features", 1.0, f"{self._stage_label('features')}{suffix}")
            self._update_stage(stage, 1.0, f"{self._stage_label(stage)}{suffix}")
            return

        if event.startswith("fetch:"):
            self._handle_fetch(event, payload)
            return
        if event.startswith("train:"):
            self._handle_train(event, payload)
            return
        if event.startswith("predict:"):
            self._handle_predict(event, payload)
            return
        if event.startswith("skills:"):
            self._handle_skills(event, payload)
            return
        if event == "bootstrap:complete":
            self._render_completion(payload["summary"])

    def _stage_label(self, stage: str) -> str:
        return {
            "fetch": "データ取得",
            "features": "特徴量作成",
            "train": "モデル学習",
            "predict": "本日の予測",
            "skills": "skill 導入",
        }[stage]

    def _stage_help(self, stage: str) -> str:
        return {
            "fetch": "\n".join(
                [
                    "リモートまたは cache から番組表、出走表、結果、オッズを取り込みます。",
                    "初回や未取得日が多い場合はネットワーク待ちが発生します。",
                    "目安: 180日分ではデータ取得だけで約1時間。履歴日数を増やすほど長くなります。",
                ]
            ),
            "features": "\n".join(
                [
                    "取得済みデータから、選手・モーター・艇番などの履歴特徴量を作成します。",
                    "過去成績を時系列で集計するため、CPU とディスクを使います。",
                    "目安: 180日分の初回全体では、データ取得後も特徴量作成と学習に追加で数十分以上かかることがあります。",
                ]
            ),
            "train": "\n".join(
                [
                    "作成済み特徴量を使って LightGBM モデルを学習し、検証します。",
                    "特徴量作成とは別工程です。サンプル数が多い場合は数分かかることがあります。",
                ]
            ),
            "predict": "\n".join(
                [
                    "学習済みモデルを使って対象日の各レースを採点します。",
                    "レースごとに勝率候補と説明用スナップショットを書き出します。",
                    "目安: 通常は数十秒から数分です。",
                ]
            ),
            "skills": "\n".join(
                [
                    "Codex、Claude Code、Claude agent から予測結果を読みやすく使えるように配置します。",
                    "Claude Code / Claude Desktop には boatrace-local MCP server も登録します。",
                    "MCP は読み取り専用で、SQL 分析も analysis_* ビューだけに制限されます。",
                    "反映には Codex / Claude Code / Claude Desktop の再起動が必要です。",
                ]
            ),
        }[stage]

    def _update_stage(self, stage: str, ratio: float, description: str) -> None:
        ratio = max(0.0, min(1.0, ratio))
        self.stage_ratios[stage] = ratio
        overall = sum(self.STAGE_WEIGHTS[name] * self.stage_ratios[name] for name in self.STAGE_WEIGHTS)
        if self.overall_task_id is not None:
            step_index = self.STAGE_ORDER.index(stage) + 1
            self.progress.update(
                self.overall_task_id,
                completed=overall,
                description=f"{step_index}/{len(self.STAGE_ORDER)} {description} | 全体 {overall:.1f}%",
            )

    def _handle_fetch(self, event: str, payload: Dict[str, Any]) -> None:
        if event == "fetch:start":
            self._update_stage("fetch", 0.0, f"データ取得 {payload['start_date']} -> {payload['end_date']}")
        elif event == "fetch:day_started":
            ratio = ((payload["current"] - 1) / max(payload["total"], 1))
            self._update_stage(
                "fetch",
                ratio,
                f"データ取得 {payload['current']}/{payload['total']}日 {payload['target_date']}",
            )
        elif event == "fetch:downloading_missing":
            self._update_stage(
                "fetch",
                max(self.stage_ratios["fetch"], (payload["current"] - 1) / max(payload["total"], 1)),
                f"不足データを取得中 {payload['target_date']}",
            )
        elif event == "fetch:day_completed":
            ratio = payload["current"] / max(payload["total"], 1)
            status = "読込" if payload["status"] == "loaded" else "欠損"
            self._update_stage(
                "fetch",
                ratio,
                f"データ取得 {payload['current']}/{payload['total']}日 {payload['target_date']} {status}",
            )
        elif event == "fetch:complete":
            summary = payload["summary"]
            self._update_stage(
                "fetch",
                1.0,
                f"データ取得 完了 {summary.get('days_processed', 0)}日処理 / {summary.get('days_missing', 0)}日欠損",
            )

    def _handle_train(self, event: str, payload: Dict[str, Any]) -> None:
        if event == "train:load_data":
            self._update_stage("features", 0.05, "学習データを読込中。件数が多いほど時間がかかります")
        elif event == "train:feature_stage":
            ratio = min(0.95, payload["step"] / max(payload["total"], 1))
            self._update_stage("features", ratio, f"特徴量作成 {payload['label']}。履歴集計中")
        elif event == "train:split_data":
            self._update_stage("features", 1.0, "特徴量作成 完了")
            self._update_stage("train", 0.15, "学習/検証に分割中")
        elif event == "train:fit_model":
            self._update_stage("train", 0.45, "LightGBM を学習中。ここも数分かかる場合があります")
        elif event == "train:evaluate_model":
            self._update_stage("train", 0.70, "検証指標を計算中")
        elif event == "train:save_model":
            self._update_stage("train", 0.85, "モデルを保存中")
        elif event in {"train:register_model", "train:registered", "train:complete"}:
            description = "モデルを登録中" if event != "train:complete" else "学習 完了"
            self._update_stage("train", 1.0, description)

    def _handle_predict(self, event: str, payload: Dict[str, Any]) -> None:
        if event == "predict:load_model":
            self.predict_dynamic_total = 6
            self._update_stage("predict", 1 / self.predict_dynamic_total, "モデルを読込中")
        elif event == "predict:load_target_races":
            self.predict_dynamic_total = max(6, int(payload["races"]) + 5)
            self._update_stage("predict", 2 / self.predict_dynamic_total, f"対象レースを読込中 {payload['races']}R")
        elif event == "predict:load_history":
            self._update_stage("predict", 3 / self.predict_dynamic_total, "履歴データを読込中")
        elif event == "predict:feature_stage":
            base = 3
            ratio = (base + (payload["step"] / max(payload["total"], 1))) / self.predict_dynamic_total
            self._update_stage("predict", ratio, f"予測特徴量を作成中 {payload['label']}")
        elif event == "predict:score_entries":
            self._update_stage("predict", 4 / self.predict_dynamic_total, "勝率スコアを計算中")
        elif event == "predict:run_started":
            total_races = max(int(payload.get("requested_races", 1)), 1)
            self.predict_dynamic_total = total_races + 5
            self._update_stage("predict", 4 / self.predict_dynamic_total, f"予測 run を開始 {total_races}R")
        elif event == "predict:race_scored":
            ratio = (4 + payload["current"]) / max(self.predict_dynamic_total, 1)
            self._update_stage(
                "predict",
                ratio,
                f"レース採点 {payload['current']}/{payload['total']} {payload['venue_code']}場 {payload['race_number']}R",
            )
        elif event == "predict:exporting":
            ratio = (self.predict_dynamic_total - 1) / max(self.predict_dynamic_total, 1)
            self._update_stage("predict", ratio, "予測スナップショットを書出中")
        elif event == "predict:complete":
            self._update_stage("predict", 1.0, f"予測 完了 {payload.get('total_races', 0)}R")

    def _handle_skills(self, event: str, payload: Dict[str, Any]) -> None:
        if event == "skills:start":
            self._update_stage("skills", 0.0, "skill 導入を開始")
        elif event == "skills:item_started":
            ratio = (payload["current"] - 1) / max(payload["total"], 1)
            self._update_stage("skills", ratio, f"導入中 {Path(payload['destination']).name}")
        elif event == "skills:item_completed":
            ratio = payload["current"] / max(payload["total"], 1)
            self._update_stage("skills", ratio, f"導入完了 {Path(payload['destination']).name}")

    def _render_completion(self, summary: Dict[str, Any]) -> None:
        if Table is None:  # pragma: no cover
            return
        table = Table(title="Bootstrap 完了")
        table.add_column("項目")
        table.add_column("内容")
        train_summary = summary.get("train") or {}
        predict_summary = summary.get("predict") or {}
        summary_paths = summary.get("summary_paths") or {}
        table.add_row("学習モデル", str(train_summary.get("model_path", "-")))
        table.add_row("予測日", str(predict_summary.get("target_date", "-")))
        table.add_row("予測出力", str((predict_summary.get("output_paths") or {}).get("markdown", "-")))
        table.add_row("summary", str(summary_paths.get("markdown", "-")))
        self.console.print(table)
        self.console.print(
            Panel.fit(
                "\n".join(
                    [
                        "BoatRace Local Predictor のセットアップは完了しました。",
                        "Codex / Claude Code から新しい skill / MCP を認識させるには、必要に応じてアプリを再起動してください。",
                        "Claude Code / Claude Desktop には boatrace-local MCP server を登録しました。",
                        "",
                        "アンインストールする場合:",
                        "  rm -rf ~/boatracedb",
                        "  rm -rf ~/.codex/skills/boatrace-predictions ~/.codex/skills/boatrace-program-sheet",
                        "  rm -rf ~/.claude/skills/boatrace-predictions ~/.claude/skills/boatrace-program-sheet",
                        "  rm -f ~/.claude/agents/boatrace-predictions.md ~/.claude/agents/boatrace-program-sheet.md",
                        "  さらに ~/.claude.json / Claude Desktop config から boatrace-local MCP を削除してください。",
                    ]
                ),
                title="セットアップ完了",
            )
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-command bootstrap for local BoatRace prediction setup"
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--cache-dir", default=DEFAULT_CACHE_DIR)
    parser.add_argument("--target-date", type=parse_date, default=date.today())
    parser.add_argument("--training-days", type=int, default=90)
    parser.add_argument(
        "--analysis-days",
        type=int,
        default=DEFAULT_ANALYSIS_DAYS,
        help="SQL分析用にDuckDBへ投入する対象日前の過去日数。最小180日。training-daysより大きい場合は取得期間も広がります。",
    )
    parser.add_argument("--retrain-interval-days", type=int, default=7)
    parser.add_argument("--download-missing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--install-codex-skills", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--install-claude-skills", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--install-claude-agents", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--install-claude-mcp", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--codex-home", type=Path, default=Path.home() / ".codex")
    parser.add_argument("--claude-home", type=Path, default=Path.home() / ".claude")
    parser.add_argument("--claude-code-config-path", type=Path, default=Path.home() / ".claude.json")
    parser.add_argument("--claude-desktop-config-path", type=Path, default=_default_claude_desktop_config_path())
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-train", action="store_true")
    parser.add_argument("--skip-predict", action="store_true")
    parser.add_argument("--skip-skill-install", action="store_true")
    parser.add_argument("--summary-dir", default="output/bootstrap")
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    return parser


def build_config(args: argparse.Namespace) -> BootstrapConfig:
    if args.training_days < 2:
        raise ValueError("training-days は 2 以上にしてください")
    if args.analysis_days < DEFAULT_ANALYSIS_DAYS:
        raise ValueError(f"analysis-days は {DEFAULT_ANALYSIS_DAYS} 以上にしてください")
    if args.analysis_days < args.training_days:
        raise ValueError("analysis-days は training-days 以上にしてください")
    if args.retrain_interval_days < 1:
        raise ValueError("retrain-interval-days は 1 以上にしてください")
    return BootstrapConfig(
        db_path=args.db_path,
        cache_dir=args.cache_dir,
        target_date=args.target_date,
        training_days=args.training_days,
        analysis_days=args.analysis_days,
        retrain_interval_days=args.retrain_interval_days,
        download_missing=args.download_missing,
        install_codex_skills=args.install_codex_skills,
        install_claude_skills=args.install_claude_skills,
        install_claude_agents=args.install_claude_agents,
        install_claude_mcp=args.install_claude_mcp,
        codex_home=args.codex_home,
        claude_home=args.claude_home,
        claude_code_config_path=args.claude_code_config_path,
        claude_desktop_config_path=args.claude_desktop_config_path,
        skip_fetch=args.skip_fetch,
        skip_train=args.skip_train,
        skip_predict=args.skip_predict,
        skip_skill_install=args.skip_skill_install,
        summary_dir=args.summary_dir,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = build_config(args)
    runner = BootstrapRunner(config=config)

    use_ui = (
        args.format == "markdown"
        and sys.stdout.isatty()
        and Console is not None
        and Progress is not None
    )
    if use_ui:
        with RichBootstrapUI() as ui:
            summary = runner.run(progress_callback=ui.callback)
    else:
        summary = runner.run()

    if args.format == "json":
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    else:
        print(runner.render_summary_markdown(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
