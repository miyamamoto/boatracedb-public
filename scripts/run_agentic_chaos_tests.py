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
        description="Persona/scenario/chaos driven conversation test harness"
    )
    parser.add_argument(
        "--fixture-root",
        default="tests/agentic/fixtures",
        help="YAML fixture directory",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    catalog_parser = subparsers.add_parser("catalog", help="List personas/scenarios/chaos profiles")
    catalog_parser.add_argument("--format", choices=["json", "markdown"], default="markdown")

    plan_parser = subparsers.add_parser("plan", help="Build a test plan")
    plan_parser.add_argument("--matrix", choices=["smoke", "recovery", "full"], default="smoke")
    plan_parser.add_argument("--scenario-id", action="append", default=[])
    plan_parser.add_argument("--chaos-id", action="append", default=[])
    plan_parser.add_argument("--include-baseline", action="store_true")
    plan_parser.add_argument("--format", choices=["json", "markdown"], default="markdown")

    bundle_parser = subparsers.add_parser("bundle", help="Write execution bundles")
    bundle_parser.add_argument("--matrix", choices=["smoke", "recovery", "full"], default="smoke")
    bundle_parser.add_argument("--scenario-id", action="append", default=[])
    bundle_parser.add_argument("--chaos-id", action="append", default=[])
    bundle_parser.add_argument("--include-baseline", action="store_true")
    bundle_parser.add_argument("--output-dir", default="output/agentic-test-bundles")

    grade_parser = subparsers.add_parser("grade", help="Grade transcripts")
    grade_group = grade_parser.add_mutually_exclusive_group(required=True)
    grade_group.add_argument("--transcript", help="Single transcript json path")
    grade_group.add_argument("--transcript-dir", help="Directory containing *.transcript.json")
    grade_parser.add_argument("--output-dir", default="output/agentic-test-results")
    grade_parser.add_argument("--format", choices=["json", "markdown"], default="markdown")

    return parser


def render_catalog_markdown(catalog: dict) -> str:
    lines = ["# Agentic Test Catalog", "", "## Personas", ""]
    for persona in catalog["personas"]:
        lines.append(
            f"- `{persona['id']}` | {persona['display_name']} | {persona['primary_goal']}"
        )
    lines.extend(["", "## Scenarios", ""])
    for scenario in catalog["scenarios"]:
        lines.append(
            f"- `{scenario['id']}` | persona={scenario['persona_id']} | {scenario['title']}"
        )
    lines.extend(["", "## Chaos Profiles", ""])
    for profile in catalog["chaos_profiles"]:
        lines.append(
            f"- `{profile['id']}` | {profile['display_name']} | {profile['description']}"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    harness = AgenticScenarioHarness(args.fixture_root)

    if args.command == "catalog":
        catalog = harness.list_catalog()
        if args.format == "json":
            print(json.dumps(catalog, ensure_ascii=False, indent=2))
        else:
            print(render_catalog_markdown(catalog))
        return 0

    if args.command == "plan":
        cases = harness.build_cases(
            matrix=args.matrix,
            scenario_ids=args.scenario_id or None,
            chaos_ids=args.chaos_id or None,
            include_baseline=args.include_baseline,
        )
        summary = harness.plan_summary(cases)
        if args.format == "json":
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print(harness.render_plan_markdown(cases))
        return 0

    if args.command == "bundle":
        cases = harness.build_cases(
            matrix=args.matrix,
            scenario_ids=args.scenario_id or None,
            chaos_ids=args.chaos_id or None,
            include_baseline=args.include_baseline,
        )
        written = harness.write_bundles(cases, args.output_dir)
        payload = {
            "success": True,
            "cases": len(written),
            "output_dir": str(Path(args.output_dir)),
            "bundles": [str(path) for path in written],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    if args.transcript:
        grades = [harness.grade_transcript(args.transcript)]
    else:
        grades = harness.grade_directory(args.transcript_dir)

    json_path = output_root / "grade-report.json"
    markdown_path = output_root / "grade-report.md"
    json_path.write_text(
        json.dumps([grade.to_dict() for grade in grades], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(harness.render_grade_markdown(grades), encoding="utf-8")
    if args.format == "json":
        print(json_path.read_text(encoding="utf-8"))
    else:
        print(markdown_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
