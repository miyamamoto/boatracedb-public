"""Daily rich prediction report builder."""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from .prediction_commentary import render_race_commentary_markdown, render_run_commentary_markdown
from .prediction_disclaimer import render_prediction_disclaimer_markdown


BODY_FONT = "HeiseiKakuGo-W5"


@dataclass
class PredictionReportOutput:
    target_date: date
    markdown_path: Path
    pdf_path: Path
    race_count: int
    venue_count: int


def build_prediction_report_data(prediction_run: Dict[str, Any]) -> Dict[str, Any]:
    races = list(prediction_run.get("races") or [])
    for race in races:
        if isinstance(race, dict):
            race.setdefault("commentary_markdown", render_race_commentary_markdown(race))

    by_confidence = sorted(races, key=lambda race: _score(race.get("confidence_score")), reverse=True)
    volatile = sorted(
        [race for race in races if 0.0 < _top_gap(race) < 0.08],
        key=lambda race: _top_gap(race),
    )
    outside = sorted(
        [race for race in races if _has_outside_top3(race)],
        key=lambda race: _best_outside_probability(race),
        reverse=True,
    )
    high_probability = sorted(
        [race for race in races if _top_probability(race) >= 0.60],
        key=_top_probability,
        reverse=True,
    )

    return {
        "run": prediction_run,
        "target_date": prediction_run.get("target_date"),
        "race_count": len(races),
        "venue_count": len({str(race.get("venue_code")).zfill(2) for race in races}),
        "run_commentary": render_run_commentary_markdown(prediction_run),
        "highlights": {
            "strongest": by_confidence[:8],
            "high_probability": high_probability[:8],
            "volatile": volatile[:8],
            "outside": outside[:8],
        },
        "aggregate_summary": _aggregate_summary(races),
        "venue_summaries": _venue_summaries(races),
    }


def render_prediction_report_markdown(report: Dict[str, Any]) -> str:
    run = report["run"]
    lines = [
        f"# BoatRace 予測レポート {report['target_date']}",
        "",
        f"- 対象レース: {report['race_count']}R",
        f"- 対象会場: {report['venue_count']}会場",
        f"- Prediction Run: `{run.get('id') or run.get('prediction_run_id') or '-'}`",
        "",
        report["run_commentary"],
        "",
        "## 注目レース",
        "",
    ]
    lines.extend(_markdown_race_list(report["highlights"]["strongest"], "confidence が高く、まず確認したいレースです。"))
    lines.extend(["", "## 軸候補が強いレース", ""])
    lines.extend(_markdown_race_list(report["highlights"]["high_probability"], "1着確率が高めで、軸候補を立てやすいレースです。"))
    lines.extend(["", "## 波乱・相手探し候補", ""])
    lines.extend(_markdown_race_list(report["highlights"]["volatile"], "上位差が小さく、頭固定に寄せすぎない方がよいレースです。"))
    lines.extend(["", "## 外枠・穴の拾いどころ", ""])
    lines.extend(_markdown_race_list(report["highlights"]["outside"], "4号艇以降が上位候補に入っているレースです。"))

    lines.extend(["", "## 集計サマリー", ""])
    lines.extend(_markdown_aggregate_summary(report["aggregate_summary"]))

    lines.extend(["", "## 会場別サマリー", ""])
    for venue in report["venue_summaries"]:
        lines.append(
            f"- {venue['venue_name']}: {venue['race_count']}R / 平均confidence {venue['avg_confidence']:.3f} / "
            f"最注目 {venue['strongest_race_label']} / 波乱候補 {venue['volatile_count']}R"
        )

    lines.extend(["", render_prediction_disclaimer_markdown().rstrip(), ""])
    return "\n".join(lines)


def write_prediction_report(
    prediction_run: Dict[str, Any],
    output_dir: Path | str,
    *,
    include_pdf: bool = True,
) -> PredictionReportOutput:
    target_date = _parse_target_date(prediction_run.get("target_date"))
    report = build_prediction_report_data(prediction_run)
    output_root = Path(output_dir) / target_date.isoformat()
    output_root.mkdir(parents=True, exist_ok=True)
    markdown_path = output_root / "prediction-report.md"
    pdf_path = output_root / "prediction-report.pdf"

    markdown_path.write_text(render_prediction_report_markdown(report), encoding="utf-8")
    if include_pdf:
        render_prediction_report_pdf(report, pdf_path)

    return PredictionReportOutput(
        target_date=target_date,
        markdown_path=markdown_path,
        pdf_path=pdf_path,
        race_count=report["race_count"],
        venue_count=report["venue_count"],
    )


def render_prediction_report_pdf(report: Dict[str, Any], output_path: Path | str) -> None:
    _register_font()
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    styles = _build_styles()
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=9 * mm,
        rightMargin=9 * mm,
        topMargin=9 * mm,
        bottomMargin=9 * mm,
        title=f"BoatRace 予測レポート {report['target_date']}",
    )
    story: List[Any] = []
    story.extend(_pdf_cover(report, styles))
    story.extend(_pdf_highlight_section("注目レース", report["highlights"]["strongest"], styles))
    story.extend(_pdf_highlight_section("軸候補が強いレース", report["highlights"]["high_probability"], styles))
    story.append(PageBreak())
    story.extend(_pdf_highlight_section("波乱・相手探し候補", report["highlights"]["volatile"], styles))
    story.extend(_pdf_highlight_section("外枠・穴の拾いどころ", report["highlights"]["outside"], styles))
    story.extend(_pdf_aggregate_summary(report["aggregate_summary"], styles))
    story.extend(_pdf_venue_summary(report["venue_summaries"], styles))
    story.extend(
        [
            Spacer(1, 3 * mm),
            Paragraph(
                "注: 予測と買い目候補は参考情報です。購入判断はオッズや直前情報も含めて自己責任でお願いします。",
                styles["small"],
            ),
        ]
    )
    doc.build(story)


def _pdf_cover(report: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> List[Any]:
    return [
        Paragraph(f"BoatRace 予測レポート {html.escape(str(report['target_date']))}", styles["title"]),
        Spacer(1, 2 * mm),
        Paragraph(f"{report['venue_count']}会場 / {report['race_count']}レース", styles["subtitle"]),
        Spacer(1, 4 * mm),
        Paragraph("今日の読み筋", styles["section"]),
        Paragraph(_plain_text(report["run_commentary"]), styles["body"]),
        Spacer(1, 4 * mm),
    ]


def _pdf_highlight_section(title: str, races: List[Dict[str, Any]], styles: Dict[str, ParagraphStyle]) -> List[Any]:
    flowables: List[Any] = [Paragraph(title, styles["section"])]
    if not races:
        flowables.extend([Paragraph("該当レースはありません。", styles["body"]), Spacer(1, 2 * mm)])
        return flowables

    rows: List[List[Any]] = [["レース", "本命", "confidence", "見立て", "上位買い目"]]
    for race in races[:8]:
        top = _top3(race)[:1]
        top_text = _top_text(top[0]) if top else "-"
        rows.append(
            [
                _race_label(race),
                top_text,
                f"{_score(race.get('confidence_score')):.3f}",
                _short_commentary(race),
                _ticket_text(race),
            ]
        )
    table = Table(rows, colWidths=[24 * mm, 24 * mm, 20 * mm, 78 * mm, 42 * mm], repeatRows=1)
    table.setStyle(_report_table_style())
    flowables.extend([table, Spacer(1, 4 * mm)])
    return flowables


def _pdf_venue_summary(venue_summaries: List[Dict[str, Any]], styles: Dict[str, ParagraphStyle]) -> List[Any]:
    rows: List[List[Any]] = [["会場", "R", "平均conf", "最注目", "波乱候補"]]
    for venue in venue_summaries:
        rows.append(
            [
                venue["venue_name"],
                str(venue["race_count"]),
                f"{venue['avg_confidence']:.3f}",
                venue["strongest_race_label"],
                f"{venue['volatile_count']}R",
            ]
        )
    table = Table(rows, colWidths=[38 * mm, 15 * mm, 26 * mm, 64 * mm, 28 * mm], repeatRows=1)
    table.setStyle(_report_table_style())
    return [Paragraph("会場別サマリー", styles["section"]), table]


def _pdf_aggregate_summary(summary: Dict[str, Any], styles: Dict[str, ParagraphStyle]) -> List[Any]:
    flowables: List[Any] = [Paragraph("集計サマリー", styles["section"])]

    confidence_rows: List[List[Any]] = [["confidence帯", "件数", "比率"]]
    for item in summary["confidence_bands"]:
        confidence_rows.append([item["label"], str(item["count"]), f"{item['share']:.1%}"])
    confidence_table = Table(confidence_rows, colWidths=[46 * mm, 20 * mm, 24 * mm], repeatRows=1)
    confidence_table.setStyle(_report_table_style())

    boat_rows: List[List[Any]] = [["本命艇番", "件数", "平均1着率"]]
    for item in summary["top_pick_boats"]:
        boat_rows.append([f"{item['boat_number']}号艇", str(item["count"]), f"{item['avg_win_probability']:.1%}"])
    boat_table = Table(boat_rows, colWidths=[36 * mm, 20 * mm, 34 * mm], repeatRows=1)
    boat_table.setStyle(_report_table_style())

    ticket_rows: List[List[Any]] = [["券種", "件数", "上位平均"]]
    for item in summary["ticket_type_summary"]:
        ticket_rows.append([item["ticket_type_label"], str(item["count"]), f"{item['avg_top_probability']:.1%}"])
    ticket_table = Table(ticket_rows, colWidths=[36 * mm, 20 * mm, 34 * mm], repeatRows=1)
    ticket_table.setStyle(_report_table_style())

    flowables.extend(
        [
            Table([[confidence_table, boat_table, ticket_table]], colWidths=[62 * mm, 62 * mm, 62 * mm]),
            Spacer(1, 4 * mm),
            Paragraph(summary["reading_note"], styles["body"]),
            Spacer(1, 4 * mm),
        ]
    )
    return flowables


def _markdown_race_list(races: List[Dict[str, Any]], empty_message: str) -> List[str]:
    if not races:
        return [f"- {empty_message} 該当レースはありません。"]
    lines: List[str] = []
    for race in races:
        lines.append(
            f"- {_race_label(race)}: {_top_line(race)} / confidence={_score(race.get('confidence_score')):.3f} / "
            f"{_ticket_text(race)}"
        )
        lines.append(f"  - {_short_commentary(race)}")
    return lines


def _markdown_aggregate_summary(summary: Dict[str, Any]) -> List[str]:
    lines = ["### confidence 帯別", ""]
    for item in summary["confidence_bands"]:
        lines.append(f"- {item['label']}: {item['count']}R ({item['share']:.1%})")

    lines.extend(["", "### 本命艇番分布", ""])
    for item in summary["top_pick_boats"]:
        lines.append(
            f"- {item['boat_number']}号艇: {item['count']}R / 平均1着率 {item['avg_win_probability']:.1%}"
        )

    lines.extend(["", "### 券種別の上位候補", ""])
    for item in summary["ticket_type_summary"]:
        lines.append(
            f"- {item['ticket_type_label']}: 対象 {item['count']}R / 上位候補の平均確率 {item['avg_top_probability']:.1%}"
        )

    lines.extend(["", f"分析メモ: {summary['reading_note']}"])
    return lines


def _aggregate_summary(races: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = max(len(races), 1)
    confidence_defs = [
        ("鉄板寄り", 0.60, 999.0),
        ("本命寄り", 0.45, 0.60),
        ("軸はいるが相手探し", 0.30, 0.45),
        ("波乱含み", -1.0, 0.30),
    ]
    confidence_bands = []
    for label, lower, upper in confidence_defs:
        count = sum(1 for race in races if lower <= _score(race.get("confidence_score")) < upper)
        confidence_bands.append({"label": label, "count": count, "share": count / total})

    boat_stats: Dict[int, List[float]] = {}
    for race in races:
        top3 = _top3(race)
        if not top3:
            continue
        boat_stats.setdefault(_boat_number(top3[0]), []).append(_score(top3[0].get("win_probability")))
    top_pick_boats = [
        {
            "boat_number": boat_number,
            "count": len(probabilities),
            "avg_win_probability": sum(probabilities) / max(len(probabilities), 1),
        }
        for boat_number, probabilities in sorted(boat_stats.items())
    ]

    ticket_type_summary = []
    for ticket_type in ("win", "exacta", "quinella", "trifecta", "trio"):
        probabilities = []
        for race in races:
            combinations = (race.get("ticket_predictions") or {}).get(ticket_type) or []
            if combinations:
                probabilities.append(_score(combinations[0].get("probability")))
        ticket_type_summary.append(
            {
                "ticket_type": ticket_type,
                "ticket_type_label": _ticket_type_label(ticket_type),
                "count": len(probabilities),
                "avg_top_probability": sum(probabilities) / max(len(probabilities), 1) if probabilities else 0.0,
            }
        )

    strong_count = confidence_bands[0]["count"] + confidence_bands[1]["count"]
    volatile_count = confidence_bands[-1]["count"]
    reading_note = (
        f"本命寄り以上は {strong_count}R、波乱含みは {volatile_count}R です。"
        "本命寄りのレースは配当が薄くなりやすく、波乱含みのレースは点数が増えやすいため、"
        "確率だけでなくオッズと購入点数のバランスで押すレースを選ぶのが現実的です。"
    )

    return {
        "confidence_bands": confidence_bands,
        "top_pick_boats": top_pick_boats,
        "ticket_type_summary": ticket_type_summary,
        "reading_note": reading_note,
    }


def _venue_summaries(races: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for race in races:
        grouped.setdefault(str(race.get("venue_code")).zfill(2), []).append(race)

    summaries: List[Dict[str, Any]] = []
    for venue_code, venue_races in grouped.items():
        strongest = max(venue_races, key=lambda race: _score(race.get("confidence_score")))
        volatile_count = sum(1 for race in venue_races if 0.0 < _top_gap(race) < 0.08)
        avg_confidence = sum(_score(race.get("confidence_score")) for race in venue_races) / max(len(venue_races), 1)
        summaries.append(
            {
                "venue_code": venue_code,
                "venue_name": strongest.get("venue_name") or venue_code,
                "race_count": len(venue_races),
                "avg_confidence": avg_confidence,
                "strongest_race_label": _race_label(strongest),
                "volatile_count": volatile_count,
            }
        )
    return sorted(summaries, key=lambda item: (-item["avg_confidence"], item["venue_code"]))


def _score(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _top3(race: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(race.get("top3") or [])[:3]


def _top_probability(race: Dict[str, Any]) -> float:
    top3 = _top3(race)
    return _score(top3[0].get("win_probability")) if top3 else 0.0


def _top_gap(race: Dict[str, Any]) -> float:
    top3 = _top3(race)
    if len(top3) < 2:
        return _top_probability(race)
    return _score(top3[0].get("win_probability")) - _score(top3[1].get("win_probability"))


def _has_outside_top3(race: Dict[str, Any]) -> bool:
    return any(_boat_number(item) >= 4 for item in _top3(race))


def _best_outside_probability(race: Dict[str, Any]) -> float:
    return max((_score(item.get("win_probability")) for item in _top3(race) if _boat_number(item) >= 4), default=0.0)


def _boat_number(item: Dict[str, Any]) -> int:
    try:
        return int(item.get("racer_id") or 0)
    except (TypeError, ValueError):
        return 0


def _race_label(race: Dict[str, Any]) -> str:
    return f"{race.get('venue_name') or race.get('venue_code')} {race.get('race_number')}R"


def _top_text(item: Dict[str, Any]) -> str:
    return f"{_boat_number(item)}号艇 {_score(item.get('win_probability')):.1%}"


def _top_line(race: Dict[str, Any]) -> str:
    top3 = _top3(race)
    if not top3:
        return "上位候補なし"
    return " / ".join(_top_text(item) for item in top3)


def _ticket_text(race: Dict[str, Any]) -> str:
    ticket_predictions = race.get("ticket_predictions") or {}
    for ticket_type in ("trifecta", "exacta", "trio", "quinella", "win"):
        combinations = ticket_predictions.get(ticket_type) or []
        if combinations:
            top = combinations[0]
            label = _ticket_type_label(ticket_type)
            return f"{label} {top.get('combination')}({_score(top.get('probability')):.1%})"
    return "買い目候補なし"


def _ticket_type_label(ticket_type: str) -> str:
    return {
        "trifecta": "3連単",
        "exacta": "2連単",
        "trio": "3連複",
        "quinella": "2連複",
        "win": "単勝",
    }.get(ticket_type, ticket_type)


def _short_commentary(race: Dict[str, Any]) -> str:
    commentary = str(race.get("commentary_markdown") or render_race_commentary_markdown(race))
    for line in commentary.splitlines():
        line = line.strip("- ").strip()
        if line and not line.startswith("#") and not line.startswith("分析メモ"):
            return line
    return _top_line(race)


def _plain_text(markdown_text: str) -> str:
    lines: List[str] = []
    for raw in markdown_text.splitlines():
        text = raw.strip()
        if not text:
            continue
        text = text.lstrip("#").strip()
        text = text.lstrip("-").strip()
        if text:
            lines.append(html.escape(text))
    return "<br/>".join(lines[:12])


def _build_styles() -> Dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "title",
            parent=sample["Title"],
            fontName=BODY_FONT,
            fontSize=20,
            leading=24,
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
        "section": ParagraphStyle(
            "section",
            parent=sample["Heading2"],
            fontName=BODY_FONT,
            fontSize=12,
            leading=15,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#132238"),
            spaceBefore=2 * mm,
            spaceAfter=1.5 * mm,
        ),
        "body": ParagraphStyle(
            "body",
            parent=sample["Normal"],
            fontName=BODY_FONT,
            fontSize=8,
            leading=11,
            alignment=TA_LEFT,
        ),
        "small": ParagraphStyle(
            "small",
            parent=sample["Normal"],
            fontName=BODY_FONT,
            fontSize=7,
            leading=9,
            alignment=TA_LEFT,
            textColor=colors.HexColor("#54606e"),
        ),
    }


def _report_table_style() -> TableStyle:
    return TableStyle(
        [
            ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#243447")),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#a5b7c8")),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#243447")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, -1), BODY_FONT),
            ("FONTSIZE", (0, 0), (-1, 0), 7.5),
            ("FONTSIZE", (0, 1), (-1, -1), 6.8),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]
    )


def _register_font() -> None:
    try:
        pdfmetrics.getFont(BODY_FONT)
    except KeyError:
        pdfmetrics.registerFont(UnicodeCIDFont(BODY_FONT))


def _parse_target_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))
