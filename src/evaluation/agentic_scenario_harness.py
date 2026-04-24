from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml


DEFAULT_FIXTURE_ROOT = Path("tests/agentic/fixtures")


@dataclass(frozen=True)
class PersonaDefinition:
    id: str
    display_name: str
    archetype: str
    experience_level: str
    primary_goal: str
    research_goals: List[str]
    output_preferences: List[str]
    frustration_points: List[str]
    likely_requests: List[str]


@dataclass(frozen=True)
class ValidationRule:
    id: str
    description: str
    target: str = "final_assistant"
    any_keywords: List[str] = field(default_factory=list)
    all_keywords: List[str] = field(default_factory=list)
    absent_keywords: List[str] = field(default_factory=list)
    artifact_patterns: List[str] = field(default_factory=list)
    artifact_must_exist: bool = False
    critical: bool = True


@dataclass(frozen=True)
class ChaosEvent:
    id: str
    step: int
    kind: str
    user_message: str
    intent: str


@dataclass(frozen=True)
class ChaosProfile:
    id: str
    display_name: str
    description: str
    intensity: str
    events: List[ChaosEvent]
    success_rules: List[ValidationRule]


@dataclass(frozen=True)
class ScenarioDefinition:
    id: str
    persona_id: str
    title: str
    user_goal: str
    initial_prompt: str
    closing_prompt: Optional[str]
    skills: List[str]
    tags: List[str]
    smoke_chaos_profile: Optional[str]
    compatible_chaos_profiles: List[str]
    success_rules: List[ValidationRule]


@dataclass(frozen=True)
class ScenarioCase:
    case_id: str
    scenario: ScenarioDefinition
    persona: PersonaDefinition
    chaos_profile: Optional[ChaosProfile]
    matrix: str

    @property
    def scenario_id(self) -> str:
        return self.scenario.id

    @property
    def chaos_profile_id(self) -> str:
        return self.chaos_profile.id if self.chaos_profile else "baseline"


@dataclass
class RuleEvaluation:
    rule_id: str
    description: str
    passed: bool
    target: str
    observed: str
    critical: bool


@dataclass
class TranscriptGrade:
    case_id: str
    scenario_id: str
    chaos_profile_id: str
    status: str
    score: float
    passed_rules: int
    failed_rules: int
    critical_failures: List[str]
    evaluations: List[RuleEvaluation]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case_id": self.case_id,
            "scenario_id": self.scenario_id,
            "chaos_profile_id": self.chaos_profile_id,
            "status": self.status,
            "score": self.score,
            "passed_rules": self.passed_rules,
            "failed_rules": self.failed_rules,
            "critical_failures": self.critical_failures,
            "evaluations": [
                {
                    "rule_id": item.rule_id,
                    "description": item.description,
                    "passed": item.passed,
                    "target": item.target,
                    "observed": item.observed,
                    "critical": item.critical,
                }
                for item in self.evaluations
            ],
        }


@dataclass
class TranscriptProgress:
    case_id: str
    scenario_id: str
    persona_id: str
    chaos_profile_id: str
    total_assistant_turns: int
    completed_assistant_turns: int
    pending_assistant_turns: int
    is_complete: bool
    next_pending: Optional[Dict[str, Any]]
    steps: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case_id": self.case_id,
            "scenario_id": self.scenario_id,
            "persona_id": self.persona_id,
            "chaos_profile_id": self.chaos_profile_id,
            "total_assistant_turns": self.total_assistant_turns,
            "completed_assistant_turns": self.completed_assistant_turns,
            "pending_assistant_turns": self.pending_assistant_turns,
            "is_complete": self.is_complete,
            "next_pending": self.next_pending,
            "steps": self.steps,
        }


class AgenticScenarioHarness:
    """Persona/scenario/chaos driven conversation test harness."""

    def __init__(self, fixture_root: Path | str = DEFAULT_FIXTURE_ROOT):
        self.fixture_root = Path(fixture_root)
        self.personas = self._load_personas()
        self.scenarios = self._load_scenarios()
        self.chaos_profiles = self._load_chaos_profiles()

    def _read_yaml(self, file_name: str) -> Dict[str, Any]:
        path = self.fixture_root / file_name
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def _acquire_transcript_lock(self, transcript_path: Path) -> Any:
        lock_path = transcript_path.with_suffix(transcript_path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+", encoding="utf-8")
        if os.name == "nt":
            # Windows CI only needs single-process recorder safety. POSIX keeps
            # advisory locking for local concurrent transcript writes.
            return handle
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def _release_transcript_lock(self, handle: Any) -> None:
        lock_path = Path(handle.name)
        if os.name == "nt":
            handle.close()
            if lock_path.exists():
                lock_path.unlink()
            return
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
        if lock_path.exists():
            lock_path.unlink()

    def _load_personas(self) -> Dict[str, PersonaDefinition]:
        payload = self._read_yaml("personas.yaml")
        personas: Dict[str, PersonaDefinition] = {}
        for item in payload.get("personas", []):
            personas[item["id"]] = PersonaDefinition(
                id=item["id"],
                display_name=item["display_name"],
                archetype=item["archetype"],
                experience_level=item["experience_level"],
                primary_goal=item["primary_goal"],
                research_goals=list(item.get("research_goals", [])),
                output_preferences=list(item.get("output_preferences", [])),
                frustration_points=list(item.get("frustration_points", [])),
                likely_requests=list(item.get("likely_requests", [])),
            )
        return personas

    def _load_scenarios(self) -> Dict[str, ScenarioDefinition]:
        payload = self._read_yaml("scenarios.yaml")
        scenarios: Dict[str, ScenarioDefinition] = {}
        for item in payload.get("scenarios", []):
            scenarios[item["id"]] = ScenarioDefinition(
                id=item["id"],
                persona_id=item["persona_id"],
                title=item["title"],
                user_goal=item["user_goal"],
                initial_prompt=item["initial_prompt"],
                closing_prompt=item.get("closing_prompt"),
                skills=list(item.get("skills", [])),
                tags=list(item.get("tags", [])),
                smoke_chaos_profile=item.get("smoke_chaos_profile"),
                compatible_chaos_profiles=list(item.get("compatible_chaos_profiles", [])),
                success_rules=self._parse_rules(item.get("success_rules", [])),
            )
        return scenarios

    def _load_chaos_profiles(self) -> Dict[str, ChaosProfile]:
        payload = self._read_yaml("chaos_profiles.yaml")
        profiles: Dict[str, ChaosProfile] = {}
        for item in payload.get("chaos_profiles", []):
            profiles[item["id"]] = ChaosProfile(
                id=item["id"],
                display_name=item["display_name"],
                description=item["description"],
                intensity=item["intensity"],
                events=[
                    ChaosEvent(
                        id=event["id"],
                        step=int(event["step"]),
                        kind=event["kind"],
                        user_message=event["user_message"],
                        intent=event["intent"],
                    )
                    for event in item.get("events", [])
                ],
                success_rules=self._parse_rules(item.get("success_rules", [])),
            )
        return profiles

    def _parse_rules(self, raw_rules: Iterable[Dict[str, Any]]) -> List[ValidationRule]:
        rules: List[ValidationRule] = []
        for item in raw_rules:
            rules.append(
                ValidationRule(
                    id=item["id"],
                    description=item["description"],
                    target=item.get("target", "final_assistant"),
                    any_keywords=[str(value) for value in item.get("any_keywords", [])],
                    all_keywords=[str(value) for value in item.get("all_keywords", [])],
                    absent_keywords=[str(value) for value in item.get("absent_keywords", [])],
                    artifact_patterns=[str(value) for value in item.get("artifact_patterns", [])],
                    artifact_must_exist=bool(item.get("artifact_must_exist", False)),
                    critical=bool(item.get("critical", True)),
                )
            )
        return rules

    def list_catalog(self) -> Dict[str, Any]:
        return {
            "personas": [
                {
                    "id": persona.id,
                    "display_name": persona.display_name,
                    "primary_goal": persona.primary_goal,
                    "research_goals": persona.research_goals,
                }
                for persona in self.personas.values()
            ],
            "scenarios": [
                {
                    "id": scenario.id,
                    "persona_id": scenario.persona_id,
                    "title": scenario.title,
                    "skills": scenario.skills,
                    "tags": scenario.tags,
                }
                for scenario in self.scenarios.values()
            ],
            "chaos_profiles": [
                {
                    "id": profile.id,
                    "display_name": profile.display_name,
                    "description": profile.description,
                    "intensity": profile.intensity,
                }
                for profile in self.chaos_profiles.values()
            ],
        }

    def build_cases(
        self,
        matrix: str = "smoke",
        scenario_ids: Optional[Iterable[str]] = None,
        chaos_ids: Optional[Iterable[str]] = None,
        include_baseline: bool = False,
    ) -> List[ScenarioCase]:
        selected_scenarios = [
            self.scenarios[scenario_id]
            for scenario_id in (scenario_ids or self.scenarios.keys())
        ]
        selected_chaos_ids = set(chaos_ids or self.chaos_profiles.keys())
        cases: List[ScenarioCase] = []

        for scenario in selected_scenarios:
            persona = self.personas[scenario.persona_id]
            chaos_candidates = [
                self.chaos_profiles[chaos_id]
                for chaos_id in scenario.compatible_chaos_profiles
                if chaos_id in selected_chaos_ids
            ]
            if include_baseline:
                cases.append(
                    ScenarioCase(
                        case_id=f"{scenario.id}--baseline",
                        scenario=scenario,
                        persona=persona,
                        chaos_profile=None,
                        matrix=matrix,
                    )
                )

            if matrix == "smoke":
                chosen_id = scenario.smoke_chaos_profile or (
                    chaos_candidates[0].id if chaos_candidates else None
                )
                chosen_profile = self.chaos_profiles[chosen_id] if chosen_id else None
                if chosen_profile:
                    cases.append(
                        ScenarioCase(
                            case_id=f"{scenario.id}--{chosen_profile.id}",
                            scenario=scenario,
                            persona=persona,
                            chaos_profile=chosen_profile,
                            matrix=matrix,
                        )
                    )
                elif not include_baseline:
                    cases.append(
                        ScenarioCase(
                            case_id=f"{scenario.id}--baseline",
                            scenario=scenario,
                            persona=persona,
                            chaos_profile=None,
                            matrix=matrix,
                        )
                    )
                continue

            if matrix == "recovery":
                for chosen_profile in chaos_candidates[:2]:
                    cases.append(
                        ScenarioCase(
                            case_id=f"{scenario.id}--{chosen_profile.id}",
                            scenario=scenario,
                            persona=persona,
                            chaos_profile=chosen_profile,
                            matrix=matrix,
                        )
                    )
                continue

            if matrix == "full":
                for chosen_profile in chaos_candidates:
                    cases.append(
                        ScenarioCase(
                            case_id=f"{scenario.id}--{chosen_profile.id}",
                            scenario=scenario,
                            persona=persona,
                            chaos_profile=chosen_profile,
                            matrix=matrix,
                        )
                    )
                if not chaos_candidates and not include_baseline:
                    cases.append(
                        ScenarioCase(
                            case_id=f"{scenario.id}--baseline",
                            scenario=scenario,
                            persona=persona,
                            chaos_profile=None,
                            matrix=matrix,
                        )
                    )
                continue

            raise ValueError(f"unsupported matrix: {matrix}")

        return cases

    def render_case_bundle(self, case: ScenarioCase) -> Dict[str, Any]:
        user_turns = [
            {
                "role": "user",
                "source": "scenario",
                "content": case.scenario.initial_prompt,
            }
        ]
        if case.chaos_profile:
            for event in sorted(case.chaos_profile.events, key=lambda item: item.step):
                user_turns.append(
                    {
                        "role": "user",
                        "source": "chaos",
                        "event_id": event.id,
                        "step": event.step,
                        "kind": event.kind,
                        "content": event.user_message,
                        "intent": event.intent,
                    }
                )
        if case.scenario.closing_prompt:
            user_turns.append(
                {
                    "role": "user",
                    "source": "scenario",
                    "content": case.scenario.closing_prompt,
                }
            )

        turns: List[Dict[str, Any]] = []
        for index, turn in enumerate(user_turns):
            turns.append(turn)
            turns.append(
                {
                    "role": "assistant",
                    "content": "",
                    "artifacts": [],
                    "notes": f"assistant reply placeholder {index + 1}",
                }
            )

        return {
            "case_id": case.case_id,
            "matrix": case.matrix,
            "persona": {
                "id": case.persona.id,
                "display_name": case.persona.display_name,
                "archetype": case.persona.archetype,
                "experience_level": case.persona.experience_level,
                "primary_goal": case.persona.primary_goal,
                "research_goals": case.persona.research_goals,
                "output_preferences": case.persona.output_preferences,
                "frustration_points": case.persona.frustration_points,
            },
            "scenario": {
                "id": case.scenario.id,
                "title": case.scenario.title,
                "user_goal": case.scenario.user_goal,
                "skills": case.scenario.skills,
                "tags": case.scenario.tags,
            },
            "chaos_profile": (
                {
                    "id": case.chaos_profile.id,
                    "display_name": case.chaos_profile.display_name,
                    "description": case.chaos_profile.description,
                    "intensity": case.chaos_profile.intensity,
                }
                if case.chaos_profile
                else None
            ),
            "validation_rules": [
                self._rule_to_dict(rule)
                for rule in (case.scenario.success_rules + (case.chaos_profile.success_rules if case.chaos_profile else []))
            ],
            "transcript_template": {
                "case_id": case.case_id,
                "scenario_id": case.scenario.id,
                "persona_id": case.persona.id,
                "chaos_profile_id": case.chaos_profile_id,
                "turns": turns,
            },
        }

    def write_bundles(
        self,
        cases: Iterable[ScenarioCase],
        output_dir: Path | str,
    ) -> List[Path]:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        written: List[Path] = []
        for case in cases:
            bundle = self.render_case_bundle(case)
            case_dir = output_root / case.case_id
            case_dir.mkdir(parents=True, exist_ok=True)
            bundle_path = case_dir / "bundle.json"
            prompt_path = case_dir / "prompt.md"
            transcript_path = case_dir / "transcript.template.json"
            bundle_path.write_text(
                json.dumps(bundle, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            prompt_path.write_text(self._render_prompt_markdown(bundle), encoding="utf-8")
            transcript_path.write_text(
                json.dumps(bundle["transcript_template"], ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            written.append(case_dir)
        return written

    def initialize_transcript(
        self,
        bundle_path: Path | str,
        output_path: Path | str,
        force: bool = False,
    ) -> Path:
        bundle_payload = json.loads(Path(bundle_path).read_text(encoding="utf-8"))
        transcript = dict(bundle_payload["transcript_template"])
        transcript["matrix"] = bundle_payload.get("matrix", transcript.get("matrix", "recorded"))
        target_path = Path(output_path)
        lock_handle = self._acquire_transcript_lock(target_path)
        try:
            if target_path.exists() and not force:
                raise FileExistsError(f"transcript already exists: {target_path}")
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(
                json.dumps(transcript, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        finally:
            self._release_transcript_lock(lock_handle)
        return target_path

    def inspect_transcript(self, transcript_path: Path | str) -> TranscriptProgress:
        transcript_payload = json.loads(Path(transcript_path).read_text(encoding="utf-8"))
        steps: List[Dict[str, Any]] = []
        user_turn: Dict[str, Any] | None = None
        assistant_index = 0
        completed = 0
        next_pending = None

        for turn_index, turn in enumerate(transcript_payload.get("turns", [])):
            if turn.get("role") == "user":
                user_turn = turn
                continue
            if turn.get("role") != "assistant":
                continue

            assistant_index += 1
            content = str(turn.get("content", "")).strip()
            is_filled = bool(content)
            if is_filled:
                completed += 1

            step = {
                "assistant_index": assistant_index,
                "turn_index": turn_index,
                "status": "completed" if is_filled else "pending",
                "user_source": (user_turn or {}).get("source", "scenario"),
                "user_event_id": (user_turn or {}).get("event_id"),
                "user_prompt": str((user_turn or {}).get("content", "")),
                "assistant_preview": content[:160],
                "artifacts": [str(item) for item in (turn.get("artifacts") or [])],
            }
            steps.append(step)

            if next_pending is None and not is_filled:
                next_pending = step

        total = assistant_index
        return TranscriptProgress(
            case_id=transcript_payload.get("case_id", ""),
            scenario_id=transcript_payload.get("scenario_id", ""),
            persona_id=transcript_payload.get("persona_id", ""),
            chaos_profile_id=transcript_payload.get("chaos_profile_id", "baseline"),
            total_assistant_turns=total,
            completed_assistant_turns=completed,
            pending_assistant_turns=total - completed,
            is_complete=completed == total,
            next_pending=next_pending,
            steps=steps,
        )

    def record_assistant_reply(
        self,
        transcript_path: Path | str,
        content: str,
        assistant_index: int | None = None,
        artifacts: Optional[Iterable[Path | str]] = None,
        append_artifacts: bool = False,
    ) -> Dict[str, Any]:
        transcript_file = Path(transcript_path)
        lock_handle = self._acquire_transcript_lock(transcript_file)
        try:
            transcript_payload = json.loads(transcript_file.read_text(encoding="utf-8"))

            assistant_turns = [
                (index, turn)
                for index, turn in enumerate(transcript_payload.get("turns", []))
                if turn.get("role") == "assistant"
            ]
            if not assistant_turns:
                raise ValueError("assistant turns not found in transcript")

            if assistant_index is None:
                target_pair = next(
                    (
                        (index, turn)
                        for index, turn in assistant_turns
                        if not str(turn.get("content", "")).strip()
                    ),
                    None,
                )
                if target_pair is None:
                    raise ValueError("no pending assistant turn found")
            else:
                if assistant_index < 1 or assistant_index > len(assistant_turns):
                    raise IndexError(f"assistant_index out of range: {assistant_index}")
                target_pair = assistant_turns[assistant_index - 1]

            turn_index, turn = target_pair
            turn["content"] = content.strip()
            normalized_artifacts = [str(item) for item in (artifacts or [])]
            if normalized_artifacts:
                existing_artifacts = [str(item) for item in (turn.get("artifacts") or [])]
                turn["artifacts"] = (
                    existing_artifacts + normalized_artifacts
                    if append_artifacts
                    else normalized_artifacts
                )
            elif not append_artifacts and "artifacts" not in turn:
                turn["artifacts"] = []

            transcript_payload["turns"][turn_index] = turn
            transcript_file.write_text(
                json.dumps(transcript_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            recorded_assistant_index = assistant_turns.index(target_pair) + 1
        finally:
            self._release_transcript_lock(lock_handle)

        progress = self.inspect_transcript(transcript_file)
        return {
            "transcript_path": str(transcript_file),
            "assistant_index": recorded_assistant_index,
            "turn_index": turn_index,
            "is_complete": progress.is_complete,
            "pending_assistant_turns": progress.pending_assistant_turns,
        }

    def grade_transcript(self, transcript_path: Path | str) -> TranscriptGrade:
        transcript_payload = json.loads(Path(transcript_path).read_text(encoding="utf-8"))
        scenario = self.scenarios[transcript_payload["scenario_id"]]
        persona = self.personas[transcript_payload["persona_id"]]
        chaos_profile = None
        chaos_profile_id = transcript_payload.get("chaos_profile_id", "baseline")
        if chaos_profile_id and chaos_profile_id != "baseline":
            chaos_profile = self.chaos_profiles[chaos_profile_id]

        case = ScenarioCase(
            case_id=transcript_payload.get("case_id", f"{scenario.id}--{chaos_profile_id}"),
            scenario=scenario,
            persona=persona,
            chaos_profile=chaos_profile,
            matrix=transcript_payload.get("matrix", "graded"),
        )

        turns = transcript_payload.get("turns", [])
        assistant_turns = [turn for turn in turns if turn.get("role") == "assistant"]
        final_assistant = assistant_turns[-1].get("content", "") if assistant_turns else ""
        assistant_text = "\n".join(str(turn.get("content", "")) for turn in assistant_turns)
        all_text = "\n".join(str(turn.get("content", "")) for turn in turns)
        artifacts = [
            str(item)
            for turn in assistant_turns
            for item in (turn.get("artifacts") or [])
        ]

        evaluations: List[RuleEvaluation] = []
        rules = case.scenario.success_rules + (case.chaos_profile.success_rules if case.chaos_profile else [])
        for rule in rules:
            passed, observed = self._evaluate_rule(
                rule=rule,
                final_assistant=final_assistant,
                assistant_text=assistant_text,
                all_text=all_text,
                artifacts=artifacts,
            )
            evaluations.append(
                RuleEvaluation(
                    rule_id=rule.id,
                    description=rule.description,
                    passed=passed,
                    target=rule.target,
                    observed=observed,
                    critical=rule.critical,
                )
            )

        passed_rules = sum(1 for item in evaluations if item.passed)
        failed_rules = len(evaluations) - passed_rules
        critical_failures = [item.rule_id for item in evaluations if item.critical and not item.passed]
        score = round((passed_rules / len(evaluations)) * 100, 1) if evaluations else 100.0
        status = "passed" if not critical_failures else "failed"

        return TranscriptGrade(
            case_id=case.case_id,
            scenario_id=case.scenario.id,
            chaos_profile_id=case.chaos_profile_id,
            status=status,
            score=score,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            critical_failures=critical_failures,
            evaluations=evaluations,
        )

    def grade_directory(self, transcript_dir: Path | str) -> List[TranscriptGrade]:
        transcript_root = Path(transcript_dir)
        grades = []
        for transcript_path in sorted(transcript_root.rglob("*.transcript.json")):
            grades.append(self.grade_transcript(transcript_path))
        return grades

    def plan_summary(self, cases: Iterable[ScenarioCase]) -> Dict[str, Any]:
        case_list = list(cases)
        by_persona: Dict[str, int] = {}
        by_chaos: Dict[str, int] = {}
        for case in case_list:
            by_persona[case.persona.display_name] = by_persona.get(case.persona.display_name, 0) + 1
            key = case.chaos_profile.display_name if case.chaos_profile else "baseline"
            by_chaos[key] = by_chaos.get(key, 0) + 1
        return {
            "total_cases": len(case_list),
            "by_persona": by_persona,
            "by_chaos_profile": by_chaos,
            "cases": [
                {
                    "case_id": case.case_id,
                    "persona": case.persona.display_name,
                    "scenario": case.scenario.title,
                    "chaos_profile": case.chaos_profile.display_name if case.chaos_profile else "baseline",
                    "skills": case.scenario.skills,
                }
                for case in case_list
            ],
        }

    def render_plan_markdown(self, cases: Iterable[ScenarioCase]) -> str:
        summary = self.plan_summary(cases)
        lines = [
            "# Agentic Scenario/Chaos Test Plan",
            "",
            f"- Total Cases: {summary['total_cases']}",
            "",
            "## Cases",
            "",
        ]
        for item in summary["cases"]:
            lines.append(
                f"- `{item['case_id']}` | {item['persona']} | {item['scenario']} | {item['chaos_profile']}"
            )
        lines.extend(["", "## Coverage", ""])
        for persona, count in summary["by_persona"].items():
            lines.append(f"- Persona {persona}: {count}")
        for chaos_name, count in summary["by_chaos_profile"].items():
            lines.append(f"- Chaos {chaos_name}: {count}")
        return "\n".join(lines) + "\n"

    def render_grade_markdown(self, grades: Iterable[TranscriptGrade]) -> str:
        grade_list = list(grades)
        lines = [
            "# Agentic Scenario/Chaos Grade Report",
            "",
            f"- Total Runs: {len(grade_list)}",
            f"- Passed: {sum(1 for item in grade_list if item.status == 'passed')}",
            f"- Failed: {sum(1 for item in grade_list if item.status != 'passed')}",
            "",
        ]
        for grade in grade_list:
            lines.append(
                f"## {grade.case_id}\n\n"
                f"- Status: {grade.status}\n"
                f"- Score: {grade.score}\n"
                f"- Passed Rules: {grade.passed_rules}\n"
                f"- Failed Rules: {grade.failed_rules}\n"
            )
            if grade.critical_failures:
                lines.append(f"- Critical Failures: {', '.join(grade.critical_failures)}\n")
        return "\n".join(lines).strip() + "\n"

    def render_transcript_status_markdown(self, progress: TranscriptProgress) -> str:
        lines = [
            "# Agentic Transcript Status",
            "",
            f"- Case: {progress.case_id}",
            f"- Scenario: {progress.scenario_id}",
            f"- Persona: {progress.persona_id}",
            f"- Chaos: {progress.chaos_profile_id}",
            f"- Completed: {progress.completed_assistant_turns}/{progress.total_assistant_turns}",
            f"- Pending: {progress.pending_assistant_turns}",
            "",
            "## Steps",
            "",
        ]
        for step in progress.steps:
            lines.append(
                f"- #{step['assistant_index']} [{step['status']}] user={step['user_prompt']}"
            )
        if progress.next_pending:
            lines.extend(
                [
                    "",
                    "## Next Pending",
                    "",
                    f"- Assistant Index: {progress.next_pending['assistant_index']}",
                    f"- User Prompt: {progress.next_pending['user_prompt']}",
                ]
            )
        return "\n".join(lines) + "\n"

    def _evaluate_rule(
        self,
        rule: ValidationRule,
        final_assistant: str,
        assistant_text: str,
        all_text: str,
        artifacts: List[str],
    ) -> tuple[bool, str]:
        if rule.target == "assistant_artifacts":
            observed = ", ".join(artifacts) if artifacts else "(none)"
            passed = True
            if rule.artifact_patterns:
                passed = any(
                    any(pattern in artifact or fnmatch(artifact, pattern) for pattern in rule.artifact_patterns)
                    for artifact in artifacts
                )
            if passed and rule.artifact_must_exist and artifacts:
                passed = any(Path(artifact).exists() for artifact in artifacts)
            return passed, observed

        target_text = {
            "final_assistant": final_assistant,
            "assistant_any": assistant_text,
            "all_text": all_text,
        }.get(rule.target, final_assistant)

        haystack = target_text.casefold()
        passed = True
        if rule.any_keywords:
            passed = any(keyword.casefold() in haystack for keyword in rule.any_keywords)
        if passed and rule.all_keywords:
            passed = all(keyword.casefold() in haystack for keyword in rule.all_keywords)
        if passed and rule.absent_keywords:
            passed = all(keyword.casefold() not in haystack for keyword in rule.absent_keywords)
        return passed, target_text[:400]

    def _render_prompt_markdown(self, bundle: Dict[str, Any]) -> str:
        persona = bundle["persona"]
        scenario = bundle["scenario"]
        chaos_profile = bundle.get("chaos_profile")
        transcript = bundle["transcript_template"]
        lines = [
            f"# {scenario['title']}",
            "",
            f"- Persona: {persona['display_name']}",
            f"- Goal: {scenario['user_goal']}",
            f"- Skills: {', '.join(scenario['skills']) or 'none'}",
        ]
        if chaos_profile:
            lines.extend(
                [
                    f"- Chaos: {chaos_profile['display_name']}",
                    f"- Intensity: {chaos_profile['intensity']}",
                ]
            )
        lines.extend(["", "## User Profile", ""])
        lines.append(f"- Archetype: {persona['archetype']}")
        lines.append(f"- Experience: {persona['experience_level']}")
        lines.append(f"- Primary Goal: {persona['primary_goal']}")
        for goal in persona["research_goals"]:
            lines.append(f"- Research Goal: {goal}")
        lines.extend(["", "## Conversation Script", ""])
        user_turn_index = 0
        for turn in transcript["turns"]:
            if turn["role"] != "user":
                continue
            user_turn_index += 1
            prefix = f"{user_turn_index}. User"
            if turn.get("source") == "chaos":
                prefix += f" [chaos:{turn.get('event_id')}]"
            lines.append(f"- {prefix}: {turn['content']}")
        lines.extend(["", "## Success Rules", ""])
        for rule in bundle["validation_rules"]:
            lines.append(f"- {rule['id']}: {rule['description']}")
        return "\n".join(lines) + "\n"

    def _rule_to_dict(self, rule: ValidationRule) -> Dict[str, Any]:
        return {
            "id": rule.id,
            "description": rule.description,
            "target": rule.target,
            "any_keywords": rule.any_keywords,
            "all_keywords": rule.all_keywords,
            "absent_keywords": rule.absent_keywords,
            "artifact_patterns": rule.artifact_patterns,
            "artifact_must_exist": rule.artifact_must_exist,
            "critical": rule.critical,
        }
