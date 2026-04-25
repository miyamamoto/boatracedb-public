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
                "top3": [{"racer_id": 1, "win_probability": 0.51}],
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
        "top3": [{"racer_id": 1, "win_probability": 0.51}],
        "ticket_predictions": {
            "trifecta": [{"combination": "1-2-3", "probability": 0.08}],
        },
    }


def test_prediction_markdown_includes_responsibility_and_odds_disclaimer():
    markdown = render_output("date", _sample_run_payload(), "markdown")

    assert "利用上の注意" in markdown
    assert "回収率を保証するものではありません" in markdown
    assert "オッズ" in markdown
    assert "あくまでレースを楽しむための材料" in markdown
    assert "自己責任" in markdown


def test_race_json_includes_disclaimer_payload():
    output = render_output("race", _sample_race_payload(), "json")

    assert '"disclaimer"' in output
    assert "回収率" in output
    assert "オッズ" in output
    assert "自己責任" in output
