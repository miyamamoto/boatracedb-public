#!/usr/bin/env python3

from __future__ import annotations

import argparse
import html
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository


BODY_FONT = "HeiseiKakuGo-W5"
BOAT_COLORS = {
    1: (colors.white, colors.black),
    2: (colors.black, colors.white),
    3: (colors.HexColor("#d71920"), colors.white),
    4: (colors.HexColor("#1456c0"), colors.white),
    5: (colors.HexColor("#f5d142"), colors.black),
    6: (colors.HexColor("#2da44e"), colors.white),
}


@dataclass
class ProgramSheetOutput:
    venue_code: str
    venue_name: str
    race_count: int
    pdf_path: Path


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate printable BoatRace program sheet PDFs")
    parser.add_argument("--db-path", default="data/boatrace_pipeline.duckdb")
    parser.add_argument("--target-date", required=True, type=parse_date)
    parser.add_argument("--venue-code", action="append", default=[])
    parser.add_argument("--output-dir", default="output/program-sheets")
    parser.add_argument("--races-per-page", type=int, default=2)
    return parser


def load_programs(
    db_path: Path | str,
    target_date: date,
    venue_codes: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    repository = DuckDBPredictionRepository(db_path, read_only=True)
    venue_codes = [str(code).zfill(2) for code in (venue_codes or [])]
    prediction_run = repository.get_predictions_for_date(target_date)
    prediction_map: Dict[tuple[str, int], Dict[str, Any]] = {}
    if prediction_run:
        for race in prediction_run.get("races", []):
            prediction_map[(str(race["venue_code"]).zfill(2), int(race["race_number"]))] = race

    with repository.connect() as conn:
        params: List[Any] = [target_date]
        venue_filter_sql = ""
        if venue_codes:
            placeholders = ", ".join(["?"] * len(venue_codes))
            venue_filter_sql = f" AND venue_code IN ({placeholders})"
            params.extend(venue_codes)

        race_rows = _fetch_dicts(
            conn.execute(
                f"""
                SELECT
                    race_date, venue_code, venue_name, race_number, race_name, grade,
                    distance, weather, wind_direction, wind_speed, wave_height,
                    water_temperature, air_temperature, vote_close_time, race_start_time,
                    tournament_name, tournament_day
                FROM races_prerace
                WHERE race_date = ? {venue_filter_sql}
                ORDER BY venue_code, race_number
                """,
                params,
            )
        )
        entry_rows = _fetch_dicts(
            conn.execute(
                f"""
                SELECT
                    race_date, venue_code, venue_name, race_number, boat_number, racer_number,
                    racer_name, age, weight, branch, racer_class, motor_number,
                    boat_equipment_number, national_win_rate, national_quinella_rate,
                    local_win_rate, local_quinella_rate, motor_quinella_rate, boat_quinella_rate
                FROM race_entries_prerace
                WHERE race_date = ? {venue_filter_sql}
                ORDER BY venue_code, race_number, boat_number
                """,
                params,
            )
        )

    if not race_rows:
        raise ValueError(f"対象日の番組表データがありません: {target_date}")

    venues: Dict[str, Dict[str, Any]] = {}
    race_index: Dict[tuple[str, int], Dict[str, Any]] = {}
    for row in race_rows:
        venue_code = str(row["venue_code"]).zfill(2)
        venue = venues.setdefault(
            venue_code,
            {
                "target_date": target_date,
                "venue_code": venue_code,
                "venue_name": row.get("venue_name") or venue_code,
                "tournament_name": row.get("tournament_name"),
                "tournament_day": row.get("tournament_day"),
                "races": [],
            },
        )
        race = dict(row)
        race["venue_code"] = venue_code
        race["entries"] = []
        race["prediction"] = prediction_map.get((venue_code, int(row["race_number"])))
        venue["races"].append(race)
        race_index[(venue_code, int(row["race_number"]))] = race

    for row in entry_rows:
        key = (str(row["venue_code"]).zfill(2), int(row["race_number"]))
        race = race_index.get(key)
        if race:
            race["entries"].append(dict(row))

    return [venues[key] for key in sorted(venues)]


def generate_program_sheet_pdfs(
    db_path: Path | str,
    target_date: date,
    output_dir: Path | str,
    venue_codes: Optional[Iterable[str]] = None,
    races_per_page: int = 2,
) -> List[ProgramSheetOutput]:
    programs = load_programs(db_path=db_path, target_date=target_date, venue_codes=venue_codes)
    output_root = Path(output_dir) / target_date.isoformat()
    output_root.mkdir(parents=True, exist_ok=True)

    _register_font()
    outputs: List[ProgramSheetOutput] = []
    for program in programs:
        venue_slug = f"{program['venue_code']}-{program['venue_name']}"
        pdf_path = output_root / f"program-sheet-{venue_slug}.pdf"
        render_program_pdf(program, pdf_path, races_per_page=max(1, races_per_page))
        outputs.append(
            ProgramSheetOutput(
                venue_code=program["venue_code"],
                venue_name=program["venue_name"],
                race_count=len(program["races"]),
                pdf_path=pdf_path,
            )
        )
    return outputs


def render_program_pdf(program: Dict[str, Any], output_path: Path, races_per_page: int = 2) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    styles = _build_styles()
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=8 * mm,
        rightMargin=8 * mm,
        topMargin=10 * mm,
        bottomMargin=10 * mm,
        title=f"{program['venue_name']} 番組表",
    )

    story: List[Any] = []
    story.extend(_build_cover_block(program, styles))

    races = program["races"]
    for index, race in enumerate(races, start=1):
        story.append(KeepTogether(_build_race_block(race, styles)))
        if index != len(races):
            story.append(Spacer(1, 4 * mm))
            if index % races_per_page == 0:
                story.append(PageBreak())

    doc.build(
        story,
        onFirstPage=lambda canvas, d: _draw_page_footer(canvas, d, program),
        onLaterPages=lambda canvas, d: _draw_page_footer(canvas, d, program),
    )


def _build_cover_block(program: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> List[Any]:
    title = Paragraph(
        html.escape(f"{program['venue_name']} 番組表"),
        styles["title"],
    )
    target_date = program["target_date"]
    sub_title_bits = [f"{target_date.year:04d}年{target_date.month:02d}月{target_date.day:02d}日"]
    if program.get("tournament_name"):
        sub_title_bits.append(str(program["tournament_name"]))
    if program.get("tournament_day"):
        sub_title_bits.append(f"{program['tournament_day']}日目")
    sub_title = Paragraph(" / ".join(sub_title_bits), styles["subtitle"])
    note = Paragraph(
        "印刷向けの固定レイアウトです。各レースに出走表と予測メモ、本線・押さえ・穴をまとめています。",
        styles["small"],
    )
    return [title, Spacer(1, 1.5 * mm), sub_title, Spacer(1, 1.5 * mm), note, Spacer(1, 4 * mm)]


def _build_race_block(race: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> List[Any]:
    flowables: List[Any] = []
    flowables.append(_build_race_header_table(race, styles))
    flowables.append(Spacer(1, 1.5 * mm))
    flowables.append(_build_entries_table(race, styles))
    flowables.append(Spacer(1, 1.5 * mm))
    flowables.append(_build_prediction_table(race, styles))
    return flowables


def _build_race_header_table(race: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> Table:
    race_title = race.get("race_name") or f"{race['race_number']}R"
    meta_bits = []
    if race.get("grade"):
        meta_bits.append(str(race["grade"]))
    if race.get("distance"):
        meta_bits.append(f"{race['distance']}m")
    if race.get("weather"):
        weather = str(race["weather"])
        if race.get("wind_speed") is not None:
            weather += f" / {float(race['wind_speed']):.1f}m"
        meta_bits.append(weather)
    if race.get("vote_close_time"):
        meta_bits.append(f"締切 {race['vote_close_time']}")

    left = Paragraph(f"<b>{race['race_number']}R</b>", styles["race_number"])
    center = Paragraph(
        f"<b>{html.escape(str(race_title))}</b><br/>{html.escape(' / '.join(meta_bits) or '番組表')}",
        styles["body"],
    )
    prediction = race.get("prediction") or {}
    confidence_label = _confidence_label(float(prediction.get("confidence_score", 0.0)))
    top_pick = _top_pick_text(prediction)
    right = Paragraph(
        f"<b>{html.escape(confidence_label)}</b><br/>{html.escape(top_pick)}",
        styles["right"],
    )
    table = Table(
        [[left, center, right]],
        colWidths=[18 * mm, 118 * mm, 50 * mm],
        rowHeights=[14 * mm],
    )
    table.setStyle(
        TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.9, colors.HexColor("#243447")),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef4fb")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return table


def _build_entries_table(race: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> Table:
    headers = ["艇", "選手", "級", "支部", "年/体", "M", "全国", "当地", "M2連", "B2連"]
    data: List[List[str]] = [headers]
    for entry in race.get("entries", []):
        data.append(
            [
                str(entry.get("boat_number") or "-"),
                str(entry.get("racer_name") or "-"),
                str(entry.get("racer_class") or "-"),
                str(entry.get("branch") or "-"),
                _format_age_weight(entry),
                _format_int(entry.get("motor_number")),
                _format_rate(entry.get("national_win_rate")),
                _format_rate(entry.get("local_win_rate")),
                _format_rate(entry.get("motor_quinella_rate")),
                _format_rate(entry.get("boat_quinella_rate")),
            ]
        )

    col_widths = [10 * mm, 36 * mm, 10 * mm, 16 * mm, 15 * mm, 10 * mm, 16 * mm, 16 * mm, 16 * mm, 16 * mm]
    table = Table(data, colWidths=col_widths)
    style_commands: List[tuple[Any, ...]] = [
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#243447")),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#a5b7c8")),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243447")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, -1), BODY_FONT),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (2, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]

    top_rows = _top_rank_rows(race.get("prediction"))
    for row_index, entry in enumerate(race.get("entries", []), start=1):
        boat_number = int(entry.get("boat_number") or 0)
        boat_bg, boat_fg = BOAT_COLORS.get(boat_number, (colors.white, colors.black))
        style_commands.append(("BACKGROUND", (0, row_index), (0, row_index), boat_bg))
        style_commands.append(("TEXTCOLOR", (0, row_index), (0, row_index), boat_fg))
        if boat_number in top_rows:
            style_commands.append(("BACKGROUND", (1, row_index), (-1, row_index), colors.HexColor("#fff7d6")))
            style_commands.append(("LINEABOVE", (0, row_index), (-1, row_index), 0.7, colors.HexColor("#d4a017")))
            style_commands.append(("LINEBELOW", (0, row_index), (-1, row_index), 0.7, colors.HexColor("#d4a017")))

    table.setStyle(TableStyle(style_commands))
    return table


def _build_prediction_table(race: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> Table:
    prediction = race.get("prediction") or {}
    summary_rows = [
        ["見立て", _describe_race(prediction)],
        ["本命", _top_pick_text(prediction)],
        ["相手", _challenger_text(prediction)],
        ["本線", _ticket_line(prediction, ("exacta", "trifecta"), (2, 1))],
        ["押さえ", _ticket_line(prediction, ("quinella", "trio"), (1, 1))],
        ["穴", _ticket_line(prediction, ("trifecta", "trio"), (2, 2))],
    ]
    table = Table(summary_rows, colWidths=[18 * mm, 168 * mm])
    table.setStyle(
        TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#243447")),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#a5b7c8")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#eef4fb")),
                ("FONTNAME", (0, 0), (-1, -1), BODY_FONT),
                ("FONTSIZE", (0, 0), (0, -1), 8),
                ("FONTSIZE", (1, 0), (1, -1), 8),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def _build_styles() -> Dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "title",
            parent=sample["Title"],
            fontName=BODY_FONT,
            fontSize=19,
            leading=22,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#132238"),
        ),
        "subtitle": ParagraphStyle(
            "subtitle",
            parent=sample["Normal"],
            fontName=BODY_FONT,
            fontSize=10,
            leading=13,
            alignment=TA_CENTER,
        ),
        "small": ParagraphStyle(
            "small",
            parent=sample["Normal"],
            fontName=BODY_FONT,
            fontSize=8,
            leading=11,
            alignment=TA_LEFT,
        ),
        "race_number": ParagraphStyle(
            "race_number",
            parent=sample["Title"],
            fontName=BODY_FONT,
            fontSize=18,
            leading=20,
            alignment=TA_CENTER,
        ),
        "body": ParagraphStyle(
            "body",
            parent=sample["Normal"],
            fontName=BODY_FONT,
            fontSize=8.5,
            leading=11,
            alignment=TA_LEFT,
        ),
        "right": ParagraphStyle(
            "right",
            parent=sample["Normal"],
            fontName=BODY_FONT,
            fontSize=8.5,
            leading=11,
            alignment=TA_RIGHT,
        ),
    }


def _draw_page_footer(canvas: Any, doc: SimpleDocTemplate, program: Dict[str, Any]) -> None:
    canvas.saveState()
    canvas.setFont(BODY_FONT, 8)
    canvas.setFillColor(colors.HexColor("#54606e"))
    footer_left = f"{program['venue_name']} 番組表 / {program['target_date'].strftime('%Y-%m-%d')}"
    canvas.drawString(doc.leftMargin, 7 * mm, footer_left)
    canvas.drawRightString(A4[0] - doc.rightMargin, 7 * mm, f"{canvas.getPageNumber()}ページ")
    canvas.restoreState()


def _describe_race(prediction: Dict[str, Any]) -> str:
    top3 = prediction.get("top3") or []
    if not top3:
        return "予測データなし。出走表中心で確認。"

    top = top3[0]
    top_boat = int(top.get("racer_id", 0))
    top_prob = float(top.get("win_probability", 0.0))
    rivals = [f"{int(item['racer_id'])}号艇" for item in top3[1:3]]
    rivals_text = "、".join(rivals) if rivals else "相手薄め"

    if top_prob >= 0.70:
        return f"{top_boat}号艇の押し切りが本線。相手は{rivals_text}。"
    if top_prob >= 0.55:
        return f"{top_boat}号艇が軸。{rivals_text}への流しが中心。"
    if top_prob >= 0.40:
        return f"{top_boat}号艇に軸は置けるが、相手探し。{rivals_text}に注意。"
    return f"混戦気配。{top_boat}号艇が一歩リードだが、{rivals_text}まで広く見たい。"


def _confidence_label(score: float) -> str:
    if score >= 0.60:
        return "鉄板寄り"
    if score >= 0.45:
        return "本命寄り"
    if score >= 0.30:
        return "軸はいる"
    return "波乱含み"


def _top_pick_text(prediction: Dict[str, Any]) -> str:
    top3 = prediction.get("top3") or []
    if not top3:
        return "予測未生成"
    item = top3[0]
    return f"{int(item['racer_id'])}号艇 {float(item['win_probability']) * 100:.1f}%"


def _challenger_text(prediction: Dict[str, Any]) -> str:
    top3 = prediction.get("top3") or []
    rivals = [f"{int(item['racer_id'])}号艇" for item in top3[1:3]]
    return " / ".join(rivals) if rivals else "相手薄め"


def _top_rank_rows(prediction: Optional[Dict[str, Any]]) -> set[int]:
    if not prediction:
        return set()
    return {int(item["racer_id"]) for item in (prediction.get("top3") or [])[:1]}


def _ticket_line(
    prediction: Dict[str, Any],
    ticket_types: tuple[str, str],
    limits: tuple[int, int],
) -> str:
    ticket_predictions = prediction.get("ticket_predictions") or {}
    bits: List[str] = []
    for ticket_type, limit in zip(ticket_types, limits):
        combos = ticket_predictions.get(ticket_type) or []
        for combo in combos[:limit]:
            bits.append(f"{combo['combination']} ({float(combo['probability']) * 100:.1f}%)")
    return " / ".join(bits) if bits else "様子見"


def _format_age_weight(entry: Dict[str, Any]) -> str:
    age = _format_int(entry.get("age"))
    weight = entry.get("weight")
    if weight in (None, ""):
        return age
    try:
        return f"{age}/{float(weight):.1f}"
    except (TypeError, ValueError):
        return age


def _format_rate(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "-"


def _format_int(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _fetch_dicts(cursor: Any) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _register_font() -> None:
    try:
        pdfmetrics.getFont(BODY_FONT)
    except KeyError:
        pdfmetrics.registerFont(UnicodeCIDFont(BODY_FONT))


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    outputs = generate_program_sheet_pdfs(
        db_path=args.db_path,
        target_date=args.target_date,
        output_dir=args.output_dir,
        venue_codes=args.venue_code,
        races_per_page=args.races_per_page,
    )
    payload = {
        "success": True,
        "target_date": args.target_date.isoformat(),
        "outputs": [
            {
                "venue_code": output.venue_code,
                "venue_name": output.venue_name,
                "race_count": output.race_count,
                "pdf_path": str(output.pdf_path),
            }
            for output in outputs
        ],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
