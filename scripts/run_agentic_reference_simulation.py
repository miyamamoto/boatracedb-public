#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.boatrace_program_sheet import generate_program_sheet_pdfs
from src.evaluation.agentic_scenario_harness import AgenticScenarioHarness, ScenarioCase
from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository


@dataclass
class RaceSummary:
    venue_name: str
    venue_code: str
    race_number: int
    confidence: float
    top3: List[Dict[str, Any]]
    ticket_predictions: Dict[str, List[Dict[str, Any]]]


class ReferenceScenarioSimulator:
    def __init__(self, db_path: Path | str = "data/boatrace_pipeline.duckdb"):
        self.db_path = Path(db_path)
        self.repository = DuckDBPredictionRepository(self.db_path, read_only=True)
        self.today = self._resolve_today()
        self.tomorrow = self.today + timedelta(days=1)

    def _resolve_today(self) -> date:
        return date.today()

    def build_transcript(self, case: ScenarioCase) -> Dict[str, Any]:
        bundle = AgenticScenarioHarness().render_case_bundle(case)
        transcript = bundle["transcript_template"]
        scenario_id = case.scenario.id

        if scenario_id == "morning-shortlist":
            self._fill_morning_shortlist(transcript, case)
        elif scenario_id == "beginner-friendly-briefing":
            self._fill_beginner_briefing(transcript, case)
        elif scenario_id == "fukuoka-program-sheet":
            self._fill_fukuoka_program_sheet(transcript, case)
        elif scenario_id == "multi-venue-comparison":
            self._fill_multi_venue_comparison(transcript, case)
        elif scenario_id == "single-race-live-check":
            self._fill_single_race_live_check(transcript, case)
        elif scenario_id == "odds-band-value-hunt":
            self._fill_odds_band_value_hunt(transcript, case)
        elif scenario_id == "travel-worthy-venues":
            self._fill_travel_worthy_venues(transcript, case)
        elif scenario_id == "creator-highlight-pack":
            self._fill_creator_highlights(transcript, case)
        elif scenario_id == "budget-controlled-ticketing":
            self._fill_budget_ticketing(transcript, case)
        elif scenario_id == "operations-readiness-check":
            self._fill_operations_readiness(transcript, case)
        else:
            raise ValueError(f"unsupported scenario: {scenario_id}")

        return transcript

    def _fill_morning_shortlist(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.tomorrow)
        top_races = self._top_races(target_date, limit=3)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = f"まず {target_date.isoformat()} の全会場から、本線候補を絞ります。"
        assistant_turns[1]["content"] = f"了解です。{target_date.isoformat()} で固定して見直します。"
        lines = [f"{target_date.isoformat()} の本線候補はこの3つです。", ""]
        for index, race in enumerate(top_races, start=1):
            lines.append(
                f"{index}番手: {race.venue_name} {race.race_number}R。"
                f"{int(race.top3[0]['racer_id'])}号艇中心で、相手は"
                f"{int(race.top3[1]['racer_id'])}号艇と{int(race.top3[2]['racer_id'])}号艇です。"
            )
        lines.extend(
            [
                "",
                f"先に見るなら {top_races[0].venue_name} {top_races[0].race_number}R、"
                f"次が {top_races[1].venue_name} {top_races[1].race_number}R、"
                f"3番手が {top_races[2].venue_name} {top_races[2].race_number}R です。",
            ]
        )
        assistant_turns[2]["content"] = "\n".join(lines)

    def _fill_beginner_briefing(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.today)
        easy_races = self._top_races(target_date, limit=3)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "見やすいレースを3つ拾います。できるだけやさしい言い方でまとめます。"
        naruto = self._find_race(target_date, venue_name="鳴門", race_number=11)
        assistant_turns[1]["content"] = (
            f"いったん鳴門11Rだけ見ると、{int(naruto.top3[0]['racer_id'])}号艇が中心です。"
            f"追いかけるなら {int(naruto.top3[1]['racer_id'])}号艇 と {int(naruto.top3[2]['racer_id'])}号艇です。"
        )
        assistant_turns[2]["content"] = "ありがとう。元の依頼に戻して、見やすいレースを選び直します。"
        lines = [f"最初の依頼に戻って全体をまとめると、{target_date.isoformat()} で初心者でも見やすいレースはこの3つです。", ""]
        for race in easy_races:
            lines.append(
                f"- {race.venue_name} {race.race_number}R: "
                f"{int(race.top3[0]['racer_id'])}号艇が先に立ちやすく、"
                f"相手は {int(race.top3[1]['racer_id'])}号艇 と {int(race.top3[2]['racer_id'])}号艇です。"
            )
        lines.append("")
        lines.append(f"家族でまず見るなら {easy_races[0].venue_name} {easy_races[0].race_number}R が一番わかりやすいです。")
        assistant_turns[3]["content"] = "\n".join(lines)

    def _fill_fukuoka_program_sheet(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.tomorrow)
        outputs = generate_program_sheet_pdfs(
            db_path=self.db_path,
            target_date=target_date,
            output_dir="output/program-sheets",
            venue_codes=["22"],
            races_per_page=2,
        )
        pdf_path = str(outputs[0].pdf_path)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = f"{target_date.isoformat()} の福岡番組表を印刷用にまとめます。"
        assistant_turns[1]["content"] = "了解です。解説よりPDF番組表を優先して仕上げます。"
        assistant_turns[1]["artifacts"] = [pdf_path]
        assistant_turns[2]["content"] = (
            f"印刷に回すファイルは福岡の番組表PDFです。{pdf_path} をそのまま使えます。"
        )
        assistant_turns[2]["artifacts"] = [pdf_path]

    def _fill_multi_venue_comparison(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.today)
        best = self._best_per_venue(target_date)
        chaotic = self._most_chaotic_per_venue(target_date)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "まず会場ごとの強い本命と混戦寄りを見比べます。"
        assistant_turns[1]["content"] = "了解です。単独会場ではなく、全会場を横並びで整理します。"
        lines = [f"{target_date.isoformat()} の全会場比較です。", ""]
        for venue_name in sorted(best):
            strong = best[venue_name]
            wild = chaotic[venue_name]
            lines.append(
                f"- {venue_name}: 強い本命は {strong.race_number}R "
                f"({int(strong.top3[0]['racer_id'])}号艇中心)。"
                f"混戦は {wild.race_number}R で、頭が割れやすいです。"
            )
        lines.append("")
        strongest_venue = max(best.values(), key=lambda item: item.confidence)
        lines.append(
            f"特に差がはっきりしている会場は {strongest_venue.venue_name} です。"
            f"{strongest_venue.race_number}R が本命寄りでした。"
        )
        assistant_turns[2]["content"] = "\n".join(lines)

    def _fill_single_race_live_check(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.tomorrow)
        race = self._find_race(target_date, venue_name="鳴門", race_number=11)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "まず鳴門11Rを見ます。"
        assistant_turns[1]["content"] = f"了解です。{target_date.isoformat()} の鳴門11Rとして見ます。"
        assistant_turns[2]["content"] = (
            f"{target_date.isoformat()} の鳴門11Rです。\n"
            f"本命: {int(race.top3[0]['racer_id'])}号艇。\n"
            f"相手: {int(race.top3[1]['racer_id'])}号艇 と {int(race.top3[2]['racer_id'])}号艇。\n"
            f"注意: 2着争いは少し広めです。\n"
            f"見送り条件: 直前気配が崩れるなら無理に絞らず見送りです。"
        )

    def _fill_odds_band_value_hunt(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.today)
        top_races = self._top_races(target_date, limit=8)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "今日の買い目候補を、低め・中穴・穴で整理します。"
        lines = [
            f"{target_date.isoformat()} はオッズがまだ揃っていないので、確率ベースで低め・中穴・穴に振り分けます。",
            "",
            f"- 低め: {top_races[0].venue_name} {top_races[0].race_number}R, {top_races[1].venue_name} {top_races[1].race_number}R",
            f"- 中穴: {top_races[2].venue_name} {top_races[2].race_number}R, {top_races[3].venue_name} {top_races[3].race_number}R, {top_races[4].venue_name} {top_races[4].race_number}R",
            f"- 穴: {top_races[5].venue_name} {top_races[5].race_number}R, {top_races[6].venue_name} {top_races[6].race_number}R",
            "",
            "中穴だけ3件に絞ると、"
            f" {top_races[2].venue_name} {top_races[2].race_number}R、"
            f"{top_races[3].venue_name} {top_races[3].race_number}R、"
            f"{top_races[4].venue_name} {top_races[4].race_number}R です。",
        ]
        assistant_turns[1]["content"] = "\n".join(lines)
        assistant_turns[2]["content"] = (
            f"{target_date.isoformat()} の低め・中穴・穴をまとめると、"
            f"低めは {top_races[0].venue_name} {top_races[0].race_number}R と {top_races[1].venue_name} {top_races[1].race_number}R、"
            f"中穴は3件で、"
            f" {top_races[2].venue_name} {top_races[2].race_number}R、"
            f"{top_races[3].venue_name} {top_races[3].race_number}R、"
            f"{top_races[4].venue_name} {top_races[4].race_number}R、"
            f"穴は {top_races[5].venue_name} {top_races[5].race_number}R です。"
            " オッズは未取得なので、確率ベースで見ています。"
        )

    def _fill_travel_worthy_venues(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.tomorrow)
        best = self._best_per_venue(target_date)
        sorted_best = sorted(best.values(), key=lambda item: item.confidence, reverse=True)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "明日の会場を、見やすさと観戦の面白さの両方で選びます。"
        assistant_turns[1]["content"] = f"了解です。{target_date.isoformat()} で固定して選び直します。"
        first = sorted_best[0]
        second = sorted_best[1]
        assistant_turns[2]["content"] = (
            f"{target_date.isoformat()} の観戦向き会場として、"
            f"第1候補は {first.venue_name}。"
            f"{first.race_number}R に強い本命があり、流れが見やすいです。\n"
            f"第2候補は {second.venue_name}。"
            f"{second.race_number}R が本命寄りで、他にも相手探しのレースがあって面白いです。"
        )

    def _fill_creator_highlights(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.today)
        top_races = self._top_races(target_date, limit=3)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "まず今日の配信ネタになりそうな見どころを拾います。"
        assistant_turns[1]["content"] = (
            f"いったん寄り道で鳴門11Rを見ると、{int(self._find_race(target_date, venue_name='鳴門', race_number=11).top3[0]['racer_id'])}号艇中心の話がしやすいです。"
        )
        assistant_turns[2]["content"] = "元の依頼に戻して、今日全体の見どころを3つに整理します。"
        lines = [f"最初の依頼に戻って全体をまとめると、{target_date.isoformat()} の配信で話しやすい見どころはこの3つです。", ""]
        for index, race in enumerate(top_races, start=1):
            lines.append(
                f"{index}. タイトル: {race.venue_name}{race.race_number}R は本命か、それとも相手が崩すか\n"
                f"   本命は {int(race.top3[0]['racer_id'])}号艇。"
                f"でも {int(race.top3[1]['racer_id'])}号艇 と {int(race.top3[2]['racer_id'])}号艇 まで話を広げやすいです。"
            )
        assistant_turns[3]["content"] = "\n".join(lines)

    def _fill_budget_ticketing(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        target_date = self._case_target_date(case, fallback=self.today)
        top_races = self._top_races(target_date, limit=4)
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "本線・押さえ・見送りに分けて、点数を増やしすぎない形で整理します。"
        lines = [
            f"{target_date.isoformat()} を 2,000円までで組み直します。",
            "",
            f"本線: {top_races[0].venue_name} {top_races[0].race_number}R, {top_races[1].venue_name} {top_races[1].race_number}R",
            f"押さえ: {top_races[2].venue_name} {top_races[2].race_number}R",
            f"見送り: {top_races[3].venue_name} {top_races[3].race_number}R は点数が広がりやすいので見送り。",
            "",
            "配分は、本線2レースに各800円、押さえに400円の2,000円です。",
        ]
        assistant_turns[1]["content"] = "\n".join(lines)
        assistant_turns[2]["content"] = (
            f"{target_date.isoformat()} を 2,000円で組むなら、"
            f"本線は {top_races[0].venue_name} {top_races[0].race_number}R と {top_races[1].venue_name} {top_races[1].race_number}R、"
            f"押さえは {top_races[2].venue_name} {top_races[2].race_number}R、"
            f"見送りは {top_races[3].venue_name} {top_races[3].race_number}R です。"
            " 配分は本線に各800円、押さえに400円です。"
        )

    def _fill_operations_readiness(self, transcript: Dict[str, Any], case: ScenarioCase) -> None:
        today_prediction = self.repository.get_predictions_for_date(self.today)
        tomorrow_prediction = self.repository.get_predictions_for_date(self.tomorrow)
        tomorrow_pdfs = list((Path("output/program-sheets") / self.tomorrow.isoformat()).glob("*.pdf"))
        today_pdfs = list((Path("output/program-sheets") / self.today.isoformat()).glob("*.pdf"))
        assistant_turns = self._assistant_turns(transcript)
        assistant_turns[0]["content"] = "まず今日と明日の準備状況を会場単位で見ます。"
        assistant_turns[1]["content"] = "了解です。福岡だけではなく、全会場の準備状況としてまとめます。"
        lines = [
            f"{self.today.isoformat()} は予測が揃っています。"
            + (" 番組表PDFも出ています。" if today_pdfs else " 番組表PDFはまだ出していません。"),
            f"{self.tomorrow.isoformat()} は予測が揃っていて、番組表PDFは {len(tomorrow_pdfs)} 会場分あります。",
            "",
            "会場単位の準備状況としては、明日分は配布向き、今日分は予測中心です。",
            "",
            "すぐ動くべき不足項目:",
            "- 今日の番組表PDFを必要な会場だけ追加生成する",
            "- 明日分で不足会場があれば追加で番組表を出す",
            "- 配布が必要な会場から優先して印刷確認する",
        ]
        assistant_turns[2]["content"] = "\n".join(lines)
        assistant_turns[2]["content"] = "全会場の準備状況をまとめると、\n" + assistant_turns[2]["content"]

    def _assistant_turns(self, transcript: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [turn for turn in transcript["turns"] if turn["role"] == "assistant"]

    def _case_target_date(self, case: ScenarioCase, fallback: date) -> date:
        for turn in AgenticScenarioHarness().render_case_bundle(case)["transcript_template"]["turns"]:
            content = str(turn.get("content", ""))
            match = re.search(r"(20\d{2}-\d{2}-\d{2})", content)
            if match:
                return date.fromisoformat(match.group(1))
        return fallback

    def _prediction_run(self, target_date: date) -> Dict[str, Any]:
        run = self.repository.get_predictions_for_date(target_date)
        if not run:
            raise ValueError(f"prediction run not found for {target_date}")
        return run

    def _top_races(self, target_date: date, limit: int) -> List[RaceSummary]:
        run = self._prediction_run(target_date)
        races = [self._to_race_summary(item) for item in run["races"]]
        return sorted(races, key=lambda item: item.confidence, reverse=True)[:limit]

    def _best_per_venue(self, target_date: date) -> Dict[str, RaceSummary]:
        run = self._prediction_run(target_date)
        best: Dict[str, RaceSummary] = {}
        for race in run["races"]:
            summary = self._to_race_summary(race)
            current = best.get(summary.venue_name)
            if current is None or summary.confidence > current.confidence:
                best[summary.venue_name] = summary
        return best

    def _most_chaotic_per_venue(self, target_date: date) -> Dict[str, RaceSummary]:
        run = self._prediction_run(target_date)
        chaotic: Dict[str, RaceSummary] = {}
        for race in run["races"]:
            summary = self._to_race_summary(race)
            current = chaotic.get(summary.venue_name)
            if current is None or summary.confidence < current.confidence:
                chaotic[summary.venue_name] = summary
        return chaotic

    def _find_race(self, target_date: date, venue_name: str, race_number: int) -> RaceSummary:
        run = self._prediction_run(target_date)
        for race in run["races"]:
            if race["venue_name"] == venue_name and int(race["race_number"]) == int(race_number):
                return self._to_race_summary(race)
        raise ValueError(f"race not found: {target_date} {venue_name} {race_number}R")

    def _to_race_summary(self, payload: Dict[str, Any]) -> RaceSummary:
        return RaceSummary(
            venue_name=str(payload["venue_name"]),
            venue_code=str(payload["venue_code"]),
            race_number=int(payload["race_number"]),
            confidence=float(payload.get("confidence_score", 0.0)),
            top3=list(payload.get("top3") or []),
            ticket_predictions=dict(payload.get("ticket_predictions") or {}),
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate reference transcripts for agentic scenario/chaos tests"
    )
    parser.add_argument("--db-path", default="data/boatrace_pipeline.duckdb")
    parser.add_argument("--matrix", choices=["smoke", "recovery", "full"], default="smoke")
    parser.add_argument("--scenario-id", action="append", default=[])
    parser.add_argument("--chaos-id", action="append", default=[])
    parser.add_argument("--include-baseline", action="store_true")
    parser.add_argument("--output-dir", default="output/agentic-test-runs")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    harness = AgenticScenarioHarness()
    simulator = ReferenceScenarioSimulator(args.db_path)
    cases = harness.build_cases(
        matrix=args.matrix,
        scenario_ids=args.scenario_id or None,
        chaos_ids=args.chaos_id or None,
        include_baseline=args.include_baseline,
    )
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    written: List[str] = []
    for case in cases:
        transcript = simulator.build_transcript(case)
        case_dir = output_root / case.case_id
        case_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = case_dir / f"{case.case_id}.transcript.json"
        transcript_path.write_text(
            json.dumps(transcript, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        written.append(str(transcript_path))

    print(
        json.dumps(
            {
                "success": True,
                "cases": len(written),
                "output_dir": str(output_root),
                "transcripts": written,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
