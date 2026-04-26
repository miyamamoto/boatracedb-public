from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from scripts.boatrace_mcp_server import _race_deep_analysis_payload
from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository


def _insert_race(conn, race_date: date, venue_code: str, race_number: int) -> None:
    conn.execute(
        """
        INSERT INTO races_prerace (
            race_date, venue_code, venue_name, race_number, race_name, grade,
            distance, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [race_date, venue_code, "平和島", race_number, "テストレース", "一般", 1800, datetime(2026, 4, 1, 9, 0)],
    )


def _insert_entry(
    conn,
    race_date: date,
    venue_code: str,
    race_number: int,
    boat_number: int,
    racer_number: int,
    motor_number: int,
) -> None:
    conn.execute(
        """
        INSERT INTO race_entries_prerace (
            race_date, venue_code, venue_name, race_number, boat_number,
            racer_number, racer_name, age, weight, branch, racer_class,
            motor_number, boat_equipment_number, national_win_rate,
            national_quinella_rate, local_win_rate, local_quinella_rate,
            motor_quinella_rate, boat_quinella_rate, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            race_date,
            venue_code,
            "平和島",
            race_number,
            boat_number,
            racer_number,
            f"選手{boat_number}",
            30 + boat_number,
            51.0,
            "東京",
            "A1" if boat_number == 1 else "B1",
            motor_number,
            20 + boat_number,
            6.0 - boat_number / 10,
            5.0 - boat_number / 10,
            5.8 - boat_number / 10,
            4.9 - boat_number / 10,
            40.0 + boat_number,
            35.0 + boat_number,
            datetime(2026, 4, 1, 9, 0),
        ],
    )


def _insert_result(
    conn,
    race_date: date,
    venue_code: str,
    race_number: int,
    boat_number: int,
    racer_number: int,
    motor_number: int,
    result_position: int,
) -> None:
    conn.execute(
        """
        INSERT INTO race_results (
            race_date, venue_code, venue_name, race_number, boat_number,
            racer_number, racer_name, motor_number, exhibition_time, st_timing,
            tilt_angle, result_position, result_time, disqualified,
            source_types_json, raw_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            race_date,
            venue_code,
            "平和島",
            race_number,
            boat_number,
            racer_number,
            f"選手{boat_number}",
            motor_number,
            6.8,
            0.12 + boat_number / 100,
            0.0,
            result_position,
            "1.49.0",
            False,
            "[]",
            "{}",
            datetime(2026, 4, 1, 10, 0),
        ],
    )


def test_race_deep_analysis_payload_returns_combined_context(tmp_path: Path) -> None:
    db_path = tmp_path / "mcp.duckdb"
    repository = DuckDBPredictionRepository(db_path)
    target_date = date(2026, 4, 26)
    historical_date = date(2026, 4, 1)

    with repository.connect() as conn:
        _insert_race(conn, target_date, "04", 9)
        _insert_race(conn, historical_date, "04", 1)
        for boat_number in range(1, 4):
            racer_number = 4000 + boat_number
            motor_number = 10 + boat_number
            _insert_entry(conn, target_date, "04", 9, boat_number, racer_number, motor_number)
            _insert_entry(conn, historical_date, "04", 1, boat_number, racer_number, motor_number)
            _insert_result(conn, historical_date, "04", 1, boat_number, racer_number, motor_number, boat_number)

    payload = _race_deep_analysis_payload(
        db_path=db_path,
        target_date="2026-04-26",
        venue_code="04",
        race_number=9,
    )

    assert payload["success"] is True
    assert "予測、出走選手" in payload["progress_label"]
    assert len(payload["entries"]) == 3
    assert len(payload["racer_summary"]) == 3
    assert len(payload["racer_venue_summary"]) == 3
    assert len(payload["motor_summary"]) == 3
