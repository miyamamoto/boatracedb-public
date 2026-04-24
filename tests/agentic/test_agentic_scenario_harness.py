from __future__ import annotations

import json
from pathlib import Path

from src.evaluation.agentic_scenario_harness import AgenticScenarioHarness


def test_catalog_loads_expected_registry_counts() -> None:
    harness = AgenticScenarioHarness()

    catalog = harness.list_catalog()

    assert len(catalog["personas"]) == 10
    assert len(catalog["scenarios"]) == 10
    assert len(catalog["chaos_profiles"]) == 6


def test_build_smoke_cases_produces_one_case_per_scenario() -> None:
    harness = AgenticScenarioHarness()

    cases = harness.build_cases(matrix="smoke")

    assert len(cases) == 10
    assert len({case.scenario_id for case in cases}) == 10
    assert all(case.chaos_profile_id != "baseline" for case in cases)


def test_write_bundles_outputs_prompt_and_transcript(tmp_path: Path) -> None:
    harness = AgenticScenarioHarness()
    cases = harness.build_cases(matrix="smoke", scenario_ids=["morning-shortlist"])

    written = harness.write_bundles(cases, tmp_path)

    assert len(written) == 1
    case_dir = written[0]
    assert (case_dir / "bundle.json").exists()
    assert (case_dir / "prompt.md").exists()
    assert (case_dir / "transcript.template.json").exists()


def test_grade_transcript_passes_pdf_artifact_case(tmp_path: Path) -> None:
    harness = AgenticScenarioHarness()
    artifact_path = tmp_path / "program-sheet-22-福岡.pdf"
    artifact_path.write_bytes(b"%PDF-1.4 sample")

    transcript = {
        "case_id": "fukuoka-program-sheet--format-switch-to-pdf",
        "scenario_id": "fukuoka-program-sheet",
        "persona_id": "print-kiosk-operator",
        "chaos_profile_id": "format-switch-to-pdf",
        "turns": [
            {
                "role": "user",
                "content": "明日の福岡の番組表を印刷用PDFで作ってください。",
            },
            {
                "role": "assistant",
                "content": "承知しました。明日の福岡の番組表を印刷用PDFでまとめます。",
                "artifacts": [],
            },
            {
                "role": "user",
                "content": "解説より、印刷に回せるPDFの番組表を優先してください。",
            },
            {
                "role": "assistant",
                "content": (
                    "福岡の番組表PDFを用意しました。印刷に回すならこのPDFを使ってください。"
                    "軽い見立ても入っています。"
                ),
                "artifacts": [str(artifact_path)],
            },
        ],
    }
    transcript_path = tmp_path / "program-sheet.transcript.json"
    transcript_path.write_text(json.dumps(transcript, ensure_ascii=False, indent=2), encoding="utf-8")

    grade = harness.grade_transcript(transcript_path)

    assert grade.status == "passed"
    assert grade.failed_rules == 0
    assert grade.score == 100.0


def test_grade_transcript_fails_when_latest_constraint_is_ignored(tmp_path: Path) -> None:
    harness = AgenticScenarioHarness()
    transcript = {
        "case_id": "operations-readiness-check--scope-expansion",
        "scenario_id": "operations-readiness-check",
        "persona_id": "multi-venue-operations-lead",
        "chaos_profile_id": "scope-expansion",
        "turns": [
            {
                "role": "user",
                "content": "今日と明日の全会場の準備状況をざっと整理してください。",
            },
            {
                "role": "assistant",
                "content": "まず福岡だけ見ます。",
                "artifacts": [],
            },
            {
                "role": "user",
                "content": "やっぱりその会場だけじゃなくて、全会場で見たいです。",
            },
            {
                "role": "assistant",
                "content": "福岡だけの準備状況を続けます。",
                "artifacts": [],
            },
        ],
    }
    transcript_path = tmp_path / "ops.transcript.json"
    transcript_path.write_text(json.dumps(transcript, ensure_ascii=False, indent=2), encoding="utf-8")

    grade = harness.grade_transcript(transcript_path)

    assert grade.status == "failed"
    assert "final-reflects-all-venues" in grade.critical_failures


def test_initialize_transcript_creates_recordable_output(tmp_path: Path) -> None:
    harness = AgenticScenarioHarness()
    cases = harness.build_cases(matrix="smoke", scenario_ids=["morning-shortlist"])
    written = harness.write_bundles(cases, tmp_path / "bundles")
    bundle_path = written[0] / "bundle.json"

    transcript_path = harness.initialize_transcript(
        bundle_path=bundle_path,
        output_path=tmp_path / "runs" / "morning-shortlist.transcript.json",
    )
    payload = json.loads(transcript_path.read_text(encoding="utf-8"))

    assert transcript_path.exists()
    assert payload["case_id"] == cases[0].case_id
    assert payload["matrix"] == "smoke"


def test_inspect_and_record_assistant_reply_updates_pending_state(tmp_path: Path) -> None:
    harness = AgenticScenarioHarness()
    cases = harness.build_cases(matrix="smoke", scenario_ids=["fukuoka-program-sheet"])
    written = harness.write_bundles(cases, tmp_path / "bundles")
    transcript_path = harness.initialize_transcript(
        bundle_path=written[0] / "bundle.json",
        output_path=tmp_path / "runs" / "fukuoka.transcript.json",
    )

    initial_progress = harness.inspect_transcript(transcript_path)

    assert initial_progress.completed_assistant_turns == 0
    assert initial_progress.pending_assistant_turns == initial_progress.total_assistant_turns
    assert initial_progress.next_pending is not None
    assert initial_progress.next_pending["assistant_index"] == 1

    artifact_path = tmp_path / "program-sheet-22-福岡.pdf"
    artifact_path.write_bytes(b"%PDF-1.4 sample")
    result = harness.record_assistant_reply(
        transcript_path=transcript_path,
        content="福岡の番組表PDFを優先して進めます。",
        artifacts=[artifact_path],
    )
    updated_progress = harness.inspect_transcript(transcript_path)
    payload = json.loads(transcript_path.read_text(encoding="utf-8"))
    first_assistant_turn = next(
        turn for turn in payload["turns"] if turn["role"] == "assistant"
    )

    assert result["assistant_index"] == 1
    assert updated_progress.completed_assistant_turns == 1
    assert updated_progress.next_pending is not None
    assert updated_progress.next_pending["assistant_index"] == 2
    assert first_assistant_turn["content"] == "福岡の番組表PDFを優先して進めます。"
    assert first_assistant_turn["artifacts"] == [str(artifact_path)]
