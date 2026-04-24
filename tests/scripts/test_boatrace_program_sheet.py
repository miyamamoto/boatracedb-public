from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from scripts.boatrace_program_sheet import generate_program_sheet_pdfs
from src.pipeline.duckdb_prediction_repository import DuckDBPredictionRepository


class _FakePrediction:
    def __init__(self) -> None:
        self.race_id = 202604242201
        self.race_date = date(2026, 4, 24)
        self.venue_code = "22"
        self.race_number = 1
        self.confidence_score = 0.62
        self.racer_predictions = {
            1: {"1位": 0.78},
            4: {"1位": 0.10},
            5: {"1位": 0.03},
            2: {"1位": 0.03},
            3: {"1位": 0.03},
            6: {"1位": 0.03},
        }
        self.ticket_probabilities = {
            "exacta": {"1-4": 0.37, "1-5": 0.12, "1-6": 0.11},
            "quinella": {"1-4": 0.46},
            "trifecta": {"1-4-5": 0.11, "1-4-6": 0.10},
            "trio": {"1-4-5": 0.22},
        }


def test_generate_program_sheet_pdf(tmp_path: Path) -> None:
    db_path = tmp_path / "program-sheet.duckdb"
    repository = DuckDBPredictionRepository(db_path)

    with repository.connect() as conn:
        conn.execute(
            """
            INSERT INTO races_prerace (
                race_date, venue_code, venue_name, race_number, race_name, grade,
                distance, vote_close_time, race_start_time, tournament_name,
                tournament_day, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date(2026, 4, 24),
                "22",
                "福岡",
                1,
                "サンプル特選",
                "一般",
                1800,
                "09:55",
                "10:00",
                "テスト開催",
                1,
                datetime(2026, 4, 23, 12, 0, 0),
            ),
        )

        entry_rows = []
        for boat_number in range(1, 7):
            entry_rows.append(
                (
                    date(2026, 4, 24),
                    "22",
                    "福岡",
                    1,
                    boat_number,
                    4000 + boat_number,
                    f"選手{boat_number}",
                    30 + boat_number,
                    51.0 + boat_number / 10,
                    "福岡",
                    "A1" if boat_number == 1 else "B1",
                    10 + boat_number,
                    20 + boat_number,
                    6.0 - boat_number / 10,
                    5.5 - boat_number / 10,
                    40.0 + boat_number,
                    35.0 + boat_number,
                    datetime(2026, 4, 23, 12, 0, 0),
                )
            )

        conn.executemany(
            """
            INSERT INTO race_entries_prerace (
                race_date, venue_code, venue_name, race_number, boat_number,
                racer_number, racer_name, age, weight, branch, racer_class,
                motor_number, boat_equipment_number, national_win_rate,
                local_win_rate, motor_quinella_rate, boat_quinella_rate, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            entry_rows,
        )

    prediction_run_id = repository.start_prediction_run(
        target_date=date(2026, 4, 24),
        model_id=None,
        model_path=None,
        requested_races=[202604242201],
    )
    repository.save_prediction_run_results(prediction_run_id, [_FakePrediction()])

    outputs = generate_program_sheet_pdfs(
        db_path=db_path,
        target_date=date(2026, 4, 24),
        output_dir=tmp_path / "program-sheets",
        venue_codes=["22"],
        races_per_page=2,
    )

    assert len(outputs) == 1
    assert outputs[0].venue_name == "福岡"
    assert outputs[0].race_count == 1
    assert outputs[0].pdf_path.exists()
    assert outputs[0].pdf_path.stat().st_size > 1000
