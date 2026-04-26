from __future__ import annotations

from pathlib import Path

from src.pipeline.prediction_report import (
    build_prediction_report_data,
    render_prediction_report_markdown,
    write_prediction_report,
)


def _race(
    venue_code: str,
    venue_name: str,
    race_number: int,
    confidence: float,
    top_probs: list[tuple[int, float]],
) -> dict:
    return {
        "venue_code": venue_code,
        "venue_name": venue_name,
        "race_number": race_number,
        "confidence_score": confidence,
        "top3": [{"racer_id": boat, "win_probability": probability} for boat, probability in top_probs],
        "ticket_predictions": {
            "trifecta": [{"combination": "1-2-3", "probability": 0.08}],
            "exacta": [{"combination": "1-2", "probability": 0.18}],
            "win": [{"combination": str(top_probs[0][0]), "probability": top_probs[0][1]}],
        },
    }


def _prediction_run() -> dict:
    return {
        "id": "run-1",
        "target_date": "2026-04-26",
        "races": [
            _race("12", "住之江", 11, 0.47, [(1, 0.74), (2, 0.09), (3, 0.07)]),
            _race("03", "江戸川", 3, 0.15, [(1, 0.45), (4, 0.15), (5, 0.13)]),
            _race("04", "平和島", 5, 0.05, [(1, 0.25), (2, 0.21), (3, 0.18)]),
        ],
    }


def test_prediction_report_markdown_includes_aggregate_sections() -> None:
    report = build_prediction_report_data(_prediction_run())
    markdown = render_prediction_report_markdown(report)

    assert "## 集計サマリー" in markdown
    assert "### confidence 帯別" in markdown
    assert "### 本命艇番分布" in markdown
    assert "### 券種別の上位候補" in markdown
    assert "会場別サマリー" in markdown
    assert "波乱含み" in markdown


def test_write_prediction_report_outputs_markdown_and_pdf(tmp_path: Path) -> None:
    output = write_prediction_report(_prediction_run(), tmp_path, include_pdf=True)

    assert output.race_count == 3
    assert output.venue_count == 3
    assert output.markdown_path.exists()
    assert output.pdf_path.exists()
    assert output.pdf_path.stat().st_size > 1000
    assert "## 集計サマリー" in output.markdown_path.read_text(encoding="utf-8")
