from scripts.boatrace_prediction_query import render_output


def _sample_run_payload():
    return {
        "id": "run-1",
        "target_date": "2026-04-26",
        "status": "completed",
        "model_path": "model.pkl",
        "total_races": 1,
        "races": [
            {
                "venue_code": "22",
                "venue_name": "福岡",
                "race_number": 12,
                "confidence_score": 0.42,
                "top3": [
                    {"racer_id": 1, "win_probability": 0.51},
                    {"racer_id": 3, "win_probability": 0.21},
                    {"racer_id": 5, "win_probability": 0.12},
                ],
                "ticket_predictions": {
                    "trifecta": [{"combination": "1-3-5", "probability": 0.08}],
                },
            }
        ],
    }


def _sample_race_payload():
    return {
        "target_date": "2026-04-26",
        "prediction_run_id": "run-1",
        "venue_code": "22",
        "venue_name": "福岡",
        "race_number": 12,
        "confidence_score": 0.42,
        "top3": [
            {"racer_id": 1, "win_probability": 0.51},
            {"racer_id": 3, "win_probability": 0.21},
            {"racer_id": 5, "win_probability": 0.12},
        ],
        "ticket_predictions": {
            "trifecta": [{"combination": "1-2-3", "probability": 0.08}],
        },
    }


def test_prediction_markdown_uses_compact_responsibility_note_and_commentary():
    markdown = render_output("date", _sample_run_payload(), "markdown")

    assert "## 今日の見立て" in markdown
    assert "### 本線候補" in markdown
    assert "分析メモ" in markdown
    assert "注:" in markdown
    assert "オッズ" in markdown
    assert "自己責任" in markdown
    assert "利用上の注意" not in markdown
    assert "回収率を保証するものではありません" not in markdown


def test_race_markdown_includes_deeper_race_reading():
    markdown = render_output("race", _sample_race_payload(), "markdown")

    assert "## 見立て" in markdown
    assert "相手候補" in markdown
    assert "買い目候補" in markdown
    assert "分析メモ" in markdown


def test_race_json_includes_compact_disclaimer_payload():
    output = render_output("race", _sample_race_payload(), "json")

    assert '"disclaimer"' in output
    assert "commentary_markdown" in output
    assert "## 見立て" in output
    assert "short_text" in output
    assert "自己責任" in output
    assert "回収率を保証するものではありません" not in output
