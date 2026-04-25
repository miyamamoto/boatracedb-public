from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from src.pipeline.local_prediction_service import LocalPredictionPipeline
from src.pipeline.local_duckdb_modeling import LocalRacePrediction, build_modeling_frame


def _build_schedule_payload(target_date: date) -> dict:
    races = []
    entries = []
    racers = []
    for race_number in (1, 2):
        races.append(
            {
                "venue_code": "07",
                "race_date": target_date,
                "race_number": race_number,
                "race_name": f"一般{race_number}",
                "grade": "一般",
                "distance": 1800,
                "wind_direction": 2,
                "wind_speed": 3.0,
                "wave_height": 1.0,
            }
        )
        for boat_number in range(1, 7):
            racer_number = 4000 + boat_number
            racers.append(
                {
                    "racer_number": racer_number,
                    "name": f"選手{boat_number}",
                    "branch": "愛知",
                    "racer_class": "A1" if boat_number <= 2 else "B1",
                }
            )
            entries.append(
                {
                    "venue_code": "07",
                    "race_date": target_date,
                    "race_number": race_number,
                    "boat_number": boat_number,
                    "racer_number": racer_number,
                    "racer_name": f"選手{boat_number}",
                    "age": 25 + boat_number,
                    "weight": 50.0 + (boat_number * 0.2),
                    "branch": "愛知",
                    "racer_class": "A1" if boat_number <= 2 else "B1",
                    "motor_number": 10 + boat_number,
                    "boat_equipment_number": 20 + boat_number,
                    "national_win_rate": 7.5 - (boat_number * 0.5),
                    "national_quinella_rate": 55.0 - (boat_number * 3.0),
                    "local_win_rate": 7.0 - (boat_number * 0.4),
                    "local_quinella_rate": 52.0 - (boat_number * 2.5),
                    "motor_quinella_rate": 45.0 - (boat_number * 2.0),
                    "boat_quinella_rate": 42.0 - (boat_number * 1.5),
                    "recent_results": "1 2 1 3" if boat_number == 1 else "3 4 3 5",
                    "tilt_angle": 0.0,
                }
            )

    return {
        "venues": [{"code": "07", "name": "蒲郡"}],
        "races": races,
        "race_entries": entries,
        "racers": racers,
    }


def _build_performance_payload(
    target_date: date,
    *,
    motor_offset: int = 10,
    exhibition_base: float = 6.45,
    st_base: float = 0.10,
) -> dict:
    races = []
    entries = []
    for race_number in (1, 2):
        races.append(
            {
                "venue_code": "07",
                "race_date": target_date,
                "race_number": race_number,
                "race_name": f"一般{race_number}",
                "grade": "一般",
                "distance": 1800,
                "weather": "晴",
                "wind_direction": 2,
                "wind_speed": 3.0,
                "wave_height": 1.0,
                "water_temperature": 20.0,
            }
        )
        for boat_number in range(1, 7):
            entries.append(
                {
                    "venue_code": "07",
                    "race_date": target_date,
                    "race_number": race_number,
                    "boat_number": boat_number,
                    "racer_number": 4000 + boat_number,
                    "motor_number": motor_offset + boat_number,
                    "exhibition_time": exhibition_base + (boat_number * 0.03),
                    "st_timing": st_base + (boat_number * 0.01),
                    "result_position": boat_number,
                    "result_time": f"1.4{boat_number}.0",
                }
            )

    return {
        "venues": [{"code": "07", "name": "蒲郡"}],
        "races": races,
        "race_entries": entries,
        "racers": [],
        "odds_data": [],
    }


def _build_dummy_prediction(target_date: date) -> LocalRacePrediction:
    return LocalRacePrediction(
        race_id=int(f"{target_date.strftime('%Y%m%d')}0701"),
        race_date=target_date,
        venue_code="07",
        race_number=1,
        racer_predictions={
            1: {
                "1位": 0.62,
                "2位": 0.18,
                "3位": 0.10,
                "racer_number": 4001,
                "racer_name": "選手1",
            },
            2: {
                "1位": 0.20,
                "2位": 0.28,
                "3位": 0.17,
                "racer_number": 4002,
                "racer_name": "選手2",
            },
            3: {
                "1位": 0.08,
                "2位": 0.16,
                "3位": 0.21,
                "racer_number": 4003,
                "racer_name": "選手3",
            },
        },
        ticket_probabilities={"win": {"1": 0.62, "2": 0.20, "3": 0.08}},
        confidence_score=0.73,
        prediction_timestamp=datetime.now(),
    )


def test_run_fetch_imports_cached_day(tmp_path):
    pipeline = LocalPredictionPipeline(tmp_path / "pipeline.duckdb")
    target_date = date(2022, 3, 20)

    def fake_load_raw_files(_cache_path, _current_target_date, _data_type):
        return ["dummy-cache-entry"]

    def fake_parse_schedule(_parser, _schedule_files, parsed_target_date):
        return _build_schedule_payload(parsed_target_date)

    def fake_parse_performance(_parser, _performance_files):
        return _build_performance_payload(target_date)

    pipeline._load_raw_files = fake_load_raw_files  # type: ignore[method-assign]
    pipeline._parse_schedule = fake_parse_schedule  # type: ignore[method-assign]
    pipeline._parse_performance = fake_parse_performance  # type: ignore[method-assign]

    result = pipeline.run_fetch(
        start_date=target_date,
        end_date=target_date,
        cache_dir="data/comprehensive_cache",
    )

    assert result["success"] is True
    status = pipeline.get_status()
    assert status["source_tables"]["races_prerace"] > 0
    assert status["source_tables"]["race_entries_prerace"] > 0
    assert status["source_date_range"]["days"] == 1


def test_train_and_predict_with_duckdb_source_data(tmp_path):
    pipeline = LocalPredictionPipeline(tmp_path / "pipeline.duckdb")

    start_date = date(2026, 4, 1)
    for offset in range(5):
        target_date = start_date + timedelta(days=offset)
        pipeline.repository.replace_source_snapshot(
            target_date=target_date,
            schedule_data=_build_schedule_payload(target_date),
            performance_data=_build_performance_payload(target_date),
        )

    prediction_target = start_date + timedelta(days=5)
    pipeline.repository.replace_source_snapshot(
        target_date=prediction_target,
        schedule_data=_build_schedule_payload(prediction_target),
        performance_data={},
    )

    training_result = pipeline.train_model(
        training_start_date=start_date,
        training_end_date=start_date + timedelta(days=4),
    )
    assert training_result["success"] is True
    assert training_result["feature_count"] > 0
    assert not any(column.startswith("racer_venue_") for column in training_result["feature_columns"])
    assert "distance" not in training_result["feature_columns"]
    assert "wind_speed" not in training_result["feature_columns"]
    assert "racer_venue_history" in training_result["feature_audit"]["retired_groups"]
    assert "distance" in training_result["feature_audit"]["retired_features"]

    prediction_result = pipeline.predict_for_date(
        target_date=prediction_target,
        limit=1,
    )
    assert prediction_result["success"] is True
    assert prediction_result["summary"]["total_races"] == 1

    race_prediction = pipeline.repository.get_race_prediction(
        target_date=prediction_target,
        venue_code="07",
        race_number=1,
    )
    assert race_prediction is not None
    assert race_prediction["top3"][0]["racer_id"] == 1


def test_predict_for_date_excludes_same_day_performance_inputs(tmp_path, monkeypatch):
    pipeline = LocalPredictionPipeline(tmp_path / "pipeline.duckdb")

    start_date = date(2026, 4, 1)
    for offset in range(3):
        target_date = start_date + timedelta(days=offset)
        pipeline.repository.replace_source_snapshot(
            target_date=target_date,
            schedule_data=_build_schedule_payload(target_date),
            performance_data=_build_performance_payload(target_date),
        )

    prediction_target = start_date + timedelta(days=3)
    pipeline.repository.replace_source_snapshot(
        target_date=prediction_target,
        schedule_data=_build_schedule_payload(prediction_target),
        performance_data=_build_performance_payload(
            prediction_target,
            motor_offset=900,
            exhibition_base=9.50,
            st_base=0.90,
        ),
    )

    captured: dict[str, object] = {}

    def fake_load_model_bundle(_model_path: str) -> dict:
        return {
            "model_type": "duckdb_lightgbm_win",
            "feature_columns": [],
            "validation_scores": {},
            "feature_importance": [],
            "model": object(),
        }

    def fake_predict_local_races(**kwargs):
        captured["target_entries_df"] = kwargs["target_entries_df"].copy()
        return [_build_dummy_prediction(prediction_target)]

    monkeypatch.setattr(
        "src.pipeline.local_duckdb_modeling.load_model_bundle",
        fake_load_model_bundle,
    )
    monkeypatch.setattr(
        "src.pipeline.local_duckdb_modeling.predict_local_races",
        fake_predict_local_races,
    )
    monkeypatch.setattr(
        LocalPredictionPipeline,
        "_export_prediction_snapshot",
        lambda *args, **kwargs: {
            "json": "ignored.json",
            "json_latest": "ignored_latest.json",
            "markdown": "ignored.md",
            "markdown_latest": "ignored_latest.md",
        },
    )

    result = pipeline.predict_for_date(
        target_date=prediction_target,
        model_path="dummy-model.pkl",
        limit=1,
    )

    assert result["success"] is True
    target_entries_df = captured["target_entries_df"].sort_values("boat_number").reset_index(drop=True)
    assert len(target_entries_df) == 6
    assert target_entries_df["motor_number"].tolist() == [11, 12, 13, 14, 15, 16]
    if "result_position" in target_entries_df.columns:
        assert target_entries_df["result_position"].isna().all()
    if "exhibition_time" in target_entries_df.columns:
        assert target_entries_df["exhibition_time"].isna().all()
    if "st_timing" in target_entries_df.columns:
        assert target_entries_df["st_timing"].isna().all()
    if "source_types_json" in target_entries_df.columns:
        assert target_entries_df["source_types_json"].fillna("").eq('[\"schedule\"]').all()


def test_train_model_fails_with_single_training_day(tmp_path):
    pipeline = LocalPredictionPipeline(tmp_path / "pipeline.duckdb")

    training_date = date(2026, 4, 1)
    pipeline.repository.replace_source_snapshot(
        target_date=training_date,
        schedule_data=_build_schedule_payload(training_date),
        performance_data=_build_performance_payload(training_date),
    )

    with pytest.raises(ValueError):
        pipeline.train_model(
            training_start_date=training_date,
            training_end_date=training_date,
        )


def test_predict_for_date_blocks_in_sample_predictions(tmp_path, monkeypatch):
    pipeline = LocalPredictionPipeline(tmp_path / "pipeline.duckdb")
    target_date = date(2026, 4, 5)

    def fake_load_model_bundle(_model_path: str) -> dict:
        return {
            "model_type": "duckdb_lightgbm_win",
            "training_end_date": target_date.isoformat(),
            "feature_columns": [],
            "validation_scores": {},
            "feature_importance": [],
            "model": object(),
        }

    monkeypatch.setattr(
        "src.pipeline.local_duckdb_modeling.load_model_bundle",
        fake_load_model_bundle,
    )

    with pytest.raises(ValueError, match="in-sample"):
        pipeline.predict_for_date(
            target_date=target_date,
            model_path="dummy-model.pkl",
        )


def test_motor_history_is_scoped_by_venue() -> None:
    entries_df = [
        {
            "race_date": date(2026, 4, 1),
            "venue_code": "07",
            "race_number": 1,
            "boat_number": 1,
            "racer_number": 4001,
            "motor_number": 12,
            "result_position": 1,
        },
        {
            "race_date": date(2026, 4, 2),
            "venue_code": "08",
            "race_number": 1,
            "boat_number": 1,
            "racer_number": 4002,
            "motor_number": 12,
            "result_position": 6,
        },
        {
            "race_date": date(2026, 4, 3),
            "venue_code": "07",
            "race_number": 1,
            "boat_number": 1,
            "racer_number": 4003,
            "motor_number": 12,
            "result_position": 2,
        },
    ]
    races_df = [
        {"race_date": item["race_date"], "venue_code": item["venue_code"], "race_number": 1}
        for item in entries_df
    ]

    frame = build_modeling_frame(
        entries_df=pd.DataFrame(entries_df),
        races_df=pd.DataFrame(races_df),
        feature_columns=["motor_count", "motor_win_rate", "motor_top3_rate", "motor_avg_finish"],
    )

    frame = frame.sort_values(["race_date", "venue_code"]).reset_index(drop=True)
    assert frame.loc[1, "venue_code"] == "08"
    assert frame.loc[1, "motor_count"] == 0
    assert frame.loc[2, "venue_code"] == "07"
    assert frame.loc[2, "motor_count"] == 1
    assert frame.loc[2, "motor_win_rate"] == 1.0


def test_race_relative_features_use_same_race_prerace_values() -> None:
    entries_df = []
    for boat_number, win_rate in enumerate([7.0, 6.0, 5.0, 4.0, 3.0, 2.0], start=1):
        entries_df.append(
            {
                "race_date": date(2026, 4, 1),
                "venue_code": "07",
                "race_number": 1,
                "boat_number": boat_number,
                "racer_number": 4000 + boat_number,
                "motor_number": 10 + boat_number,
                "national_win_rate": win_rate,
                "result_position": boat_number,
            }
        )
    races_df = [{"race_date": date(2026, 4, 1), "venue_code": "07", "race_number": 1}]

    frame = build_modeling_frame(
        entries_df=pd.DataFrame(entries_df),
        races_df=pd.DataFrame(races_df),
        feature_columns=[
            "national_win_rate",
            "national_win_rate_race_diff",
            "national_win_rate_race_gap_to_best",
        ],
    ).sort_values("boat_number")

    mean_win_rate = sum([7.0, 6.0, 5.0, 4.0, 3.0, 2.0]) / 6
    assert frame.iloc[0]["national_win_rate_race_diff"] == pytest.approx(7.0 - mean_win_rate)
    assert frame.iloc[0]["national_win_rate_race_gap_to_best"] == pytest.approx(0.0)
    assert frame.iloc[-1]["national_win_rate_race_diff"] == pytest.approx(2.0 - mean_win_rate)
    assert frame.iloc[-1]["national_win_rate_race_gap_to_best"] == pytest.approx(5.0)
