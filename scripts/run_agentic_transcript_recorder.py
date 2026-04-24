#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.evaluation.agentic_scenario_harness import AgenticScenarioHarness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Initialize, record, and finalize agentic scenario transcripts"
    )
    parser.add_argument(
        "--fixture-root",
        default="tests/agentic/fixtures",
        help="YAML fixture directory",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create a transcript from bundle.json")
    init_parser.add_argument("--bundle", required=True, help="Path to bundle.json")
    init_parser.add_argument("--output-dir", default="output/agentic-test-runs")
    init_parser.add_argument("--output-path", help="Exact transcript output path")
    init_parser.add_argument("--force", action="store_true")

    status_parser = subparsers.add_parser("status", help="Show transcript progress")
    status_parser.add_argument("--transcript", required=True)
    status_parser.add_argument("--format", choices=["json", "markdown"], default="markdown")

    reply_parser = subparsers.add_parser("reply", help="Record an assistant reply")
    reply_parser.add_argument("--transcript", required=True)
    reply_parser.add_argument("--assistant-index", type=int)
    reply_group = reply_parser.add_mutually_exclusive_group(required=True)
    reply_group.add_argument("--content", help="Assistant reply text")
    reply_group.add_argument("--content-file", help="File containing assistant reply text")
    reply_parser.add_argument("--artifact", action="append", default=[], help="Artifact path")
    reply_parser.add_argument("--append-artifacts", action="store_true")
    reply_parser.add_argument("--format", choices=["json", "markdown"], default="markdown")

    finalize_parser = subparsers.add_parser("finalize", help="Grade one transcript")
    finalize_parser.add_argument("--transcript", required=True)
    finalize_parser.add_argument("--output-dir", default="output/agentic-test-results")
    finalize_parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    finalize_parser.add_argument("--allow-incomplete", action="store_true")

    return parser


def _load_content(args: argparse.Namespace) -> str:
    if args.content is not None:
        return args.content
    return Path(args.content_file).read_text(encoding="utf-8")


def _default_output_path(bundle_path: Path, output_dir: Path) -> Path:
    bundle_payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    case_id = bundle_payload["transcript_template"]["case_id"]
    return output_dir / f"{case_id}.transcript.json"


def _render_reply_markdown(result: dict, progress_markdown: str) -> str:
    lines = [
        "# Agentic Transcript Reply Recorded",
        "",
        f"- Transcript: {result['transcript_path']}",
        f"- Assistant Index: {result['assistant_index']}",
        f"- Pending Turns: {result['pending_assistant_turns']}",
        "",
    ]
    return "\n".join(lines) + progress_markdown


def _render_finalize_markdown(grade_path: Path, summary_path: Path, status: str) -> str:
    return (
        "# Agentic Transcript Finalized\n\n"
        f"- Status: {status}\n"
        f"- JSON: {grade_path}\n"
        f"- Markdown: {summary_path}\n"
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    harness = AgenticScenarioHarness(args.fixture_root)

    if args.command == "init":
        bundle_path = Path(args.bundle)
        output_path = (
            Path(args.output_path)
            if args.output_path
            else _default_output_path(bundle_path, Path(args.output_dir))
        )
        written = harness.initialize_transcript(bundle_path, output_path, force=args.force)
        progress = harness.inspect_transcript(written)
        payload = {
            "success": True,
            "transcript_path": str(written),
            "case_id": progress.case_id,
            "pending_assistant_turns": progress.pending_assistant_turns,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.command == "status":
        progress = harness.inspect_transcript(args.transcript)
        if args.format == "json":
            print(json.dumps(progress.to_dict(), ensure_ascii=False, indent=2))
        else:
            print(harness.render_transcript_status_markdown(progress))
        return 0

    if args.command == "reply":
        content = _load_content(args)
        result = harness.record_assistant_reply(
            transcript_path=args.transcript,
            content=content,
            assistant_index=args.assistant_index,
            artifacts=args.artifact,
            append_artifacts=args.append_artifacts,
        )
        progress = harness.inspect_transcript(args.transcript)
        if args.format == "json":
            payload = dict(result)
            payload["progress"] = progress.to_dict()
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(_render_reply_markdown(result, harness.render_transcript_status_markdown(progress)))
        return 0

    progress = harness.inspect_transcript(args.transcript)
    if not progress.is_complete and not args.allow_incomplete:
        message = {
            "success": False,
            "error": "transcript is incomplete",
            "pending_assistant_turns": progress.pending_assistant_turns,
        }
        print(json.dumps(message, ensure_ascii=False, indent=2))
        return 2

    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    grade = harness.grade_transcript(args.transcript)
    grade_json_path = output_root / f"{grade.case_id}.grade.json"
    grade_markdown_path = output_root / f"{grade.case_id}.grade.md"
    grade_json_path.write_text(
        json.dumps(grade.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    grade_markdown_path.write_text(
        harness.render_grade_markdown([grade]),
        encoding="utf-8",
    )
    if args.format == "json":
        print(grade_json_path.read_text(encoding="utf-8"))
    else:
        print(_render_finalize_markdown(grade_json_path, grade_markdown_path, grade.status))
    return 0 if grade.status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
