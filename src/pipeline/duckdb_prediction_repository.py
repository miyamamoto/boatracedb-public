from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb


DEFAULT_PIPELINE_DB_PATH = Path("data/boatrace_pipeline.duckdb")

VENUE_CODE_TO_NAME = {
    "01": "桐生",
    "02": "戸田",
    "03": "江戸川",
    "04": "平和島",
    "05": "多摩川",
    "06": "浜名湖",
    "07": "蒲郡",
    "08": "常滑",
    "09": "津",
    "10": "三国",
    "11": "びわこ",
    "12": "住之江",
    "13": "尼崎",
    "14": "鳴門",
    "15": "丸亀",
    "16": "児島",
    "17": "宮島",
    "18": "徳山",
    "19": "下関",
    "20": "若松",
    "21": "芦屋",
    "22": "福岡",
    "23": "唐津",
    "24": "大村",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=_json_default)


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _clean_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class DuckDBPredictionRepository:
    """DuckDB-backed registry for local fetch, train, prediction, and source race data."""

    def __init__(
        self,
        db_path: Path | str = DEFAULT_PIPELINE_DB_PATH,
        read_only: bool = False,
    ):
        self.db_path = Path(db_path)
        self.read_only = read_only
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.read_only:
            self.initialize()

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=self.read_only)

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fetch_runs (
                    id VARCHAR PRIMARY KEY,
                    source VARCHAR NOT NULL,
                    start_date DATE,
                    end_date DATE,
                    command_json TEXT,
                    parameters_json TEXT,
                    artifacts_json TEXT,
                    status VARCHAR NOT NULL,
                    note TEXT,
                    created_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS models (
                    id VARCHAR PRIMARY KEY,
                    model_type VARCHAR NOT NULL,
                    model_path VARCHAR NOT NULL,
                    training_start_date DATE,
                    training_end_date DATE,
                    feature_count INTEGER,
                    training_samples INTEGER,
                    validation_scores_json TEXT,
                    metadata_json TEXT,
                    is_active BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS prediction_runs (
                    id VARCHAR PRIMARY KEY,
                    target_date DATE NOT NULL,
                    model_id VARCHAR,
                    model_path VARCHAR,
                    requested_races_json TEXT,
                    total_races INTEGER NOT NULL DEFAULT 0,
                    successful_races INTEGER NOT NULL DEFAULT 0,
                    failed_races INTEGER NOT NULL DEFAULT 0,
                    output_path VARCHAR,
                    summary_json TEXT,
                    status VARCHAR NOT NULL,
                    note TEXT,
                    created_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS race_predictions (
                    id VARCHAR PRIMARY KEY,
                    prediction_run_id VARCHAR NOT NULL,
                    race_id BIGINT NOT NULL,
                    target_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    venue_name VARCHAR,
                    race_number INTEGER NOT NULL,
                    confidence_score DOUBLE,
                    top_pick_racer_id BIGINT,
                    top_pick_probability DOUBLE,
                    top3_json TEXT,
                    racer_predictions_json TEXT,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ticket_predictions (
                    id VARCHAR PRIMARY KEY,
                    race_prediction_id VARCHAR NOT NULL,
                    prediction_run_id VARCHAR NOT NULL,
                    ticket_type VARCHAR NOT NULL,
                    combination VARCHAR NOT NULL,
                    probability DOUBLE NOT NULL,
                    rank_order INTEGER NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS venues (
                    venue_code VARCHAR PRIMARY KEY,
                    venue_name VARCHAR NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS racers (
                    racer_number BIGINT PRIMARY KEY,
                    racer_name VARCHAR,
                    branch VARCHAR,
                    racer_class VARCHAR,
                    last_race_date DATE,
                    last_seen_at TIMESTAMP NOT NULL,
                    metadata_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS races (
                    race_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    venue_name VARCHAR,
                    race_number INTEGER NOT NULL,
                    race_name VARCHAR,
                    grade VARCHAR,
                    distance INTEGER,
                    weather VARCHAR,
                    wind_direction VARCHAR,
                    wind_speed DOUBLE,
                    wave_height DOUBLE,
                    water_temperature DOUBLE,
                    air_temperature DOUBLE,
                    vote_close_time VARCHAR,
                    race_start_time VARCHAR,
                    tournament_name VARCHAR,
                    tournament_day INTEGER,
                    source_types_json TEXT,
                    raw_json TEXT,
                    fetched_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (race_date, venue_code, race_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS races_prerace (
                    race_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    venue_name VARCHAR,
                    race_number INTEGER NOT NULL,
                    race_name VARCHAR,
                    grade VARCHAR,
                    distance INTEGER,
                    weather VARCHAR,
                    wind_direction VARCHAR,
                    wind_speed DOUBLE,
                    wave_height DOUBLE,
                    water_temperature DOUBLE,
                    air_temperature DOUBLE,
                    vote_close_time VARCHAR,
                    race_start_time VARCHAR,
                    tournament_name VARCHAR,
                    tournament_day INTEGER,
                    source_types_json TEXT,
                    raw_json TEXT,
                    fetched_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (race_date, venue_code, race_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS race_entries (
                    race_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    venue_name VARCHAR,
                    race_number INTEGER NOT NULL,
                    boat_number INTEGER NOT NULL,
                    racer_number BIGINT,
                    racer_name VARCHAR,
                    age INTEGER,
                    weight DOUBLE,
                    branch VARCHAR,
                    racer_class VARCHAR,
                    motor_number INTEGER,
                    boat_equipment_number INTEGER,
                    boat_part INTEGER,
                    national_win_rate DOUBLE,
                    national_quinella_rate DOUBLE,
                    local_win_rate DOUBLE,
                    local_quinella_rate DOUBLE,
                    motor_quinella_rate DOUBLE,
                    boat_quinella_rate DOUBLE,
                    recent_results VARCHAR,
                    exhibition_time DOUBLE,
                    st_timing DOUBLE,
                    tilt_angle DOUBLE,
                    result_position INTEGER,
                    result_time VARCHAR,
                    disqualified BOOLEAN,
                    source_types_json TEXT,
                    raw_json TEXT,
                    fetched_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (race_date, venue_code, race_number, boat_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS race_entries_prerace (
                    race_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    venue_name VARCHAR,
                    race_number INTEGER NOT NULL,
                    boat_number INTEGER NOT NULL,
                    racer_number BIGINT,
                    racer_name VARCHAR,
                    age INTEGER,
                    weight DOUBLE,
                    branch VARCHAR,
                    racer_class VARCHAR,
                    motor_number INTEGER,
                    boat_equipment_number INTEGER,
                    boat_part INTEGER,
                    national_win_rate DOUBLE,
                    national_quinella_rate DOUBLE,
                    local_win_rate DOUBLE,
                    local_quinella_rate DOUBLE,
                    motor_quinella_rate DOUBLE,
                    boat_quinella_rate DOUBLE,
                    recent_results VARCHAR,
                    exhibition_time DOUBLE,
                    st_timing DOUBLE,
                    tilt_angle DOUBLE,
                    source_types_json TEXT,
                    raw_json TEXT,
                    fetched_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (race_date, venue_code, race_number, boat_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS race_results (
                    race_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    venue_name VARCHAR,
                    race_number INTEGER NOT NULL,
                    boat_number INTEGER NOT NULL,
                    racer_number BIGINT,
                    racer_name VARCHAR,
                    motor_number INTEGER,
                    exhibition_time DOUBLE,
                    st_timing DOUBLE,
                    tilt_angle DOUBLE,
                    result_position INTEGER,
                    result_time VARCHAR,
                    disqualified BOOLEAN,
                    source_types_json TEXT,
                    raw_json TEXT,
                    fetched_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (race_date, venue_code, race_number, boat_number)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS odds_data (
                    race_date DATE NOT NULL,
                    venue_code VARCHAR NOT NULL,
                    race_number INTEGER NOT NULL,
                    ticket_type VARCHAR NOT NULL,
                    combination VARCHAR NOT NULL,
                    odds DOUBLE,
                    payout INTEGER,
                    source_types_json TEXT,
                    raw_json TEXT,
                    fetched_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (race_date, venue_code, race_number, ticket_type, combination)
                )
                """
            )
            self._create_analysis_views(conn)

    def _create_analysis_views(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            """
            CREATE OR REPLACE VIEW analysis_racer_results AS
            SELECT
                e.race_date,
                e.venue_code,
                e.venue_name,
                e.race_number,
                e.boat_number,
                e.racer_number,
                e.racer_name,
                e.branch,
                e.racer_class,
                e.motor_number,
                e.boat_equipment_number,
                e.national_win_rate,
                e.national_quinella_rate,
                e.local_win_rate,
                e.local_quinella_rate,
                e.motor_quinella_rate,
                e.boat_quinella_rate,
                r.grade,
                r.distance,
                r.race_name,
                rr.exhibition_time,
                rr.st_timing,
                rr.result_position,
                rr.result_time,
                COALESCE(rr.disqualified, FALSE) AS disqualified
            FROM race_entries_prerace AS e
            LEFT JOIN races_prerace AS r
                ON e.race_date = r.race_date
               AND e.venue_code = r.venue_code
               AND e.race_number = r.race_number
            LEFT JOIN race_results AS rr
                ON e.race_date = rr.race_date
               AND e.venue_code = rr.venue_code
               AND e.race_number = rr.race_number
               AND e.boat_number = rr.boat_number
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analysis_racer_summary AS
            SELECT
                racer_number,
                any_value(racer_name) AS racer_name,
                any_value(branch) AS branch,
                any_value(racer_class) AS racer_class,
                COUNT(*) AS starts,
                SUM(CASE WHEN result_position = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result_position <= 2 THEN 1 ELSE 0 END) AS top2,
                SUM(CASE WHEN result_position <= 3 THEN 1 ELSE 0 END) AS top3,
                AVG(result_position) AS avg_finish,
                AVG(st_timing) AS avg_st,
                CAST(SUM(CASE WHEN result_position = 1 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS win_rate,
                CAST(SUM(CASE WHEN result_position <= 2 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS top2_rate,
                CAST(SUM(CASE WHEN result_position <= 3 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS top3_rate,
                MIN(race_date) AS first_race_date,
                MAX(race_date) AS latest_race_date
            FROM analysis_racer_results
            WHERE racer_number IS NOT NULL
              AND result_position IS NOT NULL
            GROUP BY racer_number
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analysis_racer_venue_summary AS
            SELECT
                racer_number,
                any_value(racer_name) AS racer_name,
                venue_code,
                any_value(venue_name) AS venue_name,
                COUNT(*) AS starts,
                SUM(CASE WHEN result_position = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result_position <= 2 THEN 1 ELSE 0 END) AS top2,
                SUM(CASE WHEN result_position <= 3 THEN 1 ELSE 0 END) AS top3,
                AVG(result_position) AS avg_finish,
                AVG(st_timing) AS avg_st,
                CAST(SUM(CASE WHEN result_position = 1 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS win_rate,
                CAST(SUM(CASE WHEN result_position <= 2 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS top2_rate,
                CAST(SUM(CASE WHEN result_position <= 3 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS top3_rate,
                MAX(race_date) AS latest_race_date
            FROM analysis_racer_results
            WHERE racer_number IS NOT NULL
              AND venue_code IS NOT NULL
              AND result_position IS NOT NULL
            GROUP BY racer_number, venue_code
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analysis_motor_summary AS
            SELECT
                venue_code,
                any_value(venue_name) AS venue_name,
                motor_number,
                COUNT(*) AS starts,
                SUM(CASE WHEN result_position = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result_position <= 2 THEN 1 ELSE 0 END) AS top2,
                SUM(CASE WHEN result_position <= 3 THEN 1 ELSE 0 END) AS top3,
                AVG(result_position) AS avg_finish,
                AVG(st_timing) AS avg_st,
                CAST(SUM(CASE WHEN result_position = 1 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS win_rate,
                CAST(SUM(CASE WHEN result_position <= 2 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS top2_rate,
                CAST(SUM(CASE WHEN result_position <= 3 THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS top3_rate,
                MAX(race_date) AS latest_race_date
            FROM analysis_racer_results
            WHERE venue_code IS NOT NULL
              AND motor_number IS NOT NULL
              AND result_position IS NOT NULL
            GROUP BY venue_code, motor_number
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW analysis_race_calendar AS
            SELECT
                r.race_date,
                r.venue_code,
                r.venue_name,
                r.race_number,
                r.race_name,
                r.grade,
                r.distance,
                r.race_start_time,
                COUNT(DISTINCT e.boat_number) AS entry_count,
                COUNT(DISTINCT rr.boat_number) AS result_count
            FROM races_prerace AS r
            LEFT JOIN race_entries_prerace AS e
                ON r.race_date = e.race_date
               AND r.venue_code = e.venue_code
               AND r.race_number = e.race_number
            LEFT JOIN race_results AS rr
                ON r.race_date = rr.race_date
               AND r.venue_code = rr.venue_code
               AND r.race_number = rr.race_number
            GROUP BY
                r.race_date,
                r.venue_code,
                r.venue_name,
                r.race_number,
                r.race_name,
                r.grade,
                r.distance,
                r.race_start_time
            """
        )

    @staticmethod
    def _rows_to_dicts(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def _decode_json(value: Any) -> Any:
        if value in (None, ""):
            return None
        return json.loads(value)

    def _upsert_venues(
        self,
        conn: duckdb.DuckDBPyConnection,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return
        conn.executemany(
            """
            INSERT OR REPLACE INTO venues (venue_code, venue_name, updated_at)
            VALUES (?, ?, ?)
            """,
            [
                (row["venue_code"], row["venue_name"], row["updated_at"])
                for row in rows
            ],
        )

    def _upsert_racers(
        self,
        conn: duckdb.DuckDBPyConnection,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return
        conn.executemany(
            """
            INSERT OR REPLACE INTO racers (
                racer_number, racer_name, branch, racer_class,
                last_race_date, last_seen_at, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["racer_number"],
                    row["racer_name"],
                    row["branch"],
                    row["racer_class"],
                    row["last_race_date"],
                    row["last_seen_at"],
                    row["metadata_json"],
                )
                for row in rows
            ],
        )

    def _insert_races(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return
        conn.executemany(
            f"""
            INSERT INTO {table_name} (
                race_date, venue_code, venue_name, race_number, race_name, grade,
                distance, weather, wind_direction, wind_speed, wave_height,
                water_temperature, air_temperature, vote_close_time, race_start_time,
                tournament_name, tournament_day, source_types_json, raw_json, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["race_date"],
                    row["venue_code"],
                    row.get("venue_name"),
                    row["race_number"],
                    row.get("race_name"),
                    row.get("grade"),
                    row.get("distance"),
                    row.get("weather"),
                    row.get("wind_direction"),
                    row.get("wind_speed"),
                    row.get("wave_height"),
                    row.get("water_temperature"),
                    row.get("air_temperature"),
                    row.get("vote_close_time"),
                    row.get("race_start_time"),
                    row.get("tournament_name"),
                    row.get("tournament_day"),
                    row["source_types_json"],
                    row.get("raw_json"),
                    row["fetched_at"],
                )
                for row in rows
            ],
        )

    def _insert_entries(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        rows: List[Dict[str, Any]],
        include_results: bool = False,
    ) -> None:
        if not rows:
            return
        result_columns = ""
        result_placeholders = ""
        if include_results:
            result_columns = ", result_position, result_time, disqualified"
            result_placeholders = ", ?, ?, ?"
        conn.executemany(
            f"""
            INSERT INTO {table_name} (
                race_date, venue_code, venue_name, race_number, boat_number,
                racer_number, racer_name, age, weight, branch, racer_class,
                motor_number, boat_equipment_number, boat_part,
                national_win_rate, national_quinella_rate, local_win_rate,
                local_quinella_rate, motor_quinella_rate, boat_quinella_rate,
                recent_results, exhibition_time, st_timing, tilt_angle
                {result_columns},
                source_types_json, raw_json, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                {result_placeholders}, ?, ?, ?)
            """,
            [
                (
                    row["race_date"],
                    row["venue_code"],
                    row.get("venue_name"),
                    row["race_number"],
                    row["boat_number"],
                    row.get("racer_number"),
                    row.get("racer_name"),
                    row.get("age"),
                    row.get("weight"),
                    row.get("branch"),
                    row.get("racer_class"),
                    row.get("motor_number"),
                    row.get("boat_equipment_number"),
                    row.get("boat_part"),
                    row.get("national_win_rate"),
                    row.get("national_quinella_rate"),
                    row.get("local_win_rate"),
                    row.get("local_quinella_rate"),
                    row.get("motor_quinella_rate"),
                    row.get("boat_quinella_rate"),
                    row.get("recent_results"),
                    row.get("exhibition_time"),
                    row.get("st_timing"),
                    row.get("tilt_angle"),
                    *(
                        (
                            row.get("result_position"),
                            row.get("result_time"),
                            row.get("disqualified"),
                        )
                        if include_results
                        else ()
                    ),
                    row["source_types_json"],
                    row.get("raw_json"),
                    row["fetched_at"],
                )
                for row in rows
            ],
        )

    def _insert_race_results(
        self,
        conn: duckdb.DuckDBPyConnection,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return
        conn.executemany(
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
                (
                    row["race_date"],
                    row["venue_code"],
                    row.get("venue_name"),
                    row["race_number"],
                    row["boat_number"],
                    row.get("racer_number"),
                    row.get("racer_name"),
                    row.get("motor_number"),
                    row.get("exhibition_time"),
                    row.get("st_timing"),
                    row.get("tilt_angle"),
                    row.get("result_position"),
                    row.get("result_time"),
                    row.get("disqualified"),
                    row["source_types_json"],
                    row.get("raw_json"),
                    row["fetched_at"],
                )
                for row in rows
            ],
        )

    def _insert_odds(
        self,
        conn: duckdb.DuckDBPyConnection,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return
        conn.executemany(
            """
            INSERT INTO odds_data (
                race_date, venue_code, race_number, ticket_type, combination,
                odds, payout, source_types_json, raw_json, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["race_date"],
                    row["venue_code"],
                    row["race_number"],
                    row["ticket_type"],
                    row["combination"],
                    row.get("odds"),
                    row.get("payout"),
                    row["source_types_json"],
                    row.get("raw_json"),
                    row["fetched_at"],
                )
                for row in rows
            ],
        )

    def replace_source_snapshot(
        self,
        target_date: date,
        schedule_data: Optional[Dict[str, Any]] = None,
        performance_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        schedule_snapshot = self._normalize_source_payload(
            target_date=target_date,
            source_type="schedule",
            payload=schedule_data or {},
        )
        performance_snapshot = self._normalize_source_payload(
            target_date=target_date,
            source_type="performance",
            payload=performance_data or {},
        )
        merged = self._merge_source_payloads(
            target_date=target_date,
            schedule_data=schedule_data or {},
            performance_data=performance_data or {},
        )

        with self.connect() as conn:
            conn.execute("DELETE FROM races WHERE race_date = ?", (target_date,))
            conn.execute("DELETE FROM races_prerace WHERE race_date = ?", (target_date,))
            conn.execute("DELETE FROM race_entries WHERE race_date = ?", (target_date,))
            conn.execute("DELETE FROM race_entries_prerace WHERE race_date = ?", (target_date,))
            conn.execute("DELETE FROM race_results WHERE race_date = ?", (target_date,))
            conn.execute("DELETE FROM odds_data WHERE race_date = ?", (target_date,))

            self._upsert_venues(conn, merged["venues"])
            self._upsert_racers(conn, merged["racers"])
            self._insert_races(conn, "races", merged["races"])
            self._insert_races(conn, "races_prerace", schedule_snapshot["races"])
            self._insert_entries(conn, "race_entries", merged["race_entries"], include_results=True)
            self._insert_entries(
                conn,
                "race_entries_prerace",
                schedule_snapshot["race_entries"],
                include_results=False,
            )
            self._insert_race_results(conn, performance_snapshot["race_entries"])
            self._insert_odds(conn, merged["odds_data"])

        return {
            "venues": len(merged["venues"]),
            "racers": len(merged["racers"]),
            "races": len(merged["races"]),
            "races_prerace": len(schedule_snapshot["races"]),
            "race_entries": len(merged["race_entries"]),
            "race_entries_prerace": len(schedule_snapshot["race_entries"]),
            "race_results": len(performance_snapshot["race_entries"]),
            "odds_data": len(merged["odds_data"]),
        }

    def _normalize_source_payload(
        self,
        target_date: date,
        source_type: str,
        payload: Dict[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        if not payload:
            return {
                "venues": [],
                "racers": [],
                "races": [],
                "race_entries": [],
                "odds_data": [],
            }

        fetched_at = _now()
        venue_map: Dict[str, Dict[str, Any]] = {}
        racer_map: Dict[int, Dict[str, Any]] = {}
        race_map: Dict[tuple[date, str, int], Dict[str, Any]] = {}
        entry_map: Dict[tuple[date, str, int, int], Dict[str, Any]] = {}
        odds_map: Dict[tuple[date, str, int, str, str], Dict[str, Any]] = {}

        def merge_non_empty(existing: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
            for key, value in updates.items():
                if key == "source_types":
                    existing.setdefault("source_types", set()).update(value)
                    continue
                if value not in (None, "", []):
                    existing[key] = value
            return existing

        for venue in payload.get("venues", []):
            venue_code = _clean_text(venue.get("code"))
            if not venue_code:
                continue
            venue_map[venue_code] = {
                "venue_code": venue_code,
                "venue_name": _clean_text(venue.get("name")) or VENUE_CODE_TO_NAME.get(venue_code, venue_code),
                "updated_at": fetched_at,
            }

        for racer in payload.get("racers", []):
            racer_number = _clean_int(racer.get("racer_number"))
            if racer_number is None:
                continue
            existing = racer_map.setdefault(
                racer_number,
                {
                    "racer_number": racer_number,
                    "racer_name": None,
                    "branch": None,
                    "racer_class": None,
                    "last_race_date": target_date,
                    "last_seen_at": fetched_at,
                    "metadata_json": _json_dumps({}),
                },
            )
            merge_non_empty(
                existing,
                {
                    "racer_name": _clean_text(racer.get("name")),
                    "branch": _clean_text(racer.get("branch")),
                    "racer_class": _clean_text(racer.get("racer_class")),
                },
            )
            existing["last_race_date"] = target_date
            existing["last_seen_at"] = fetched_at
            existing["metadata_json"] = _json_dumps(racer)

        for race in payload.get("races", []):
            venue_code = _clean_text(race.get("venue_code"))
            race_number = _clean_int(race.get("race_number"))
            if not venue_code or race_number is None:
                continue
            key = (target_date, venue_code, race_number)
            base = race_map.setdefault(
                key,
                {
                    "race_date": target_date,
                    "venue_code": venue_code,
                    "venue_name": venue_map.get(venue_code, {}).get("venue_name", VENUE_CODE_TO_NAME.get(venue_code, venue_code)),
                    "race_number": race_number,
                    "source_types": set(),
                    "fetched_at": fetched_at,
                },
            )
            merge_non_empty(
                base,
                {
                    "venue_name": venue_map.get(venue_code, {}).get("venue_name"),
                    "race_name": _clean_text(race.get("race_name")),
                    "grade": _clean_text(race.get("grade")),
                    "distance": _clean_int(race.get("distance")),
                    "weather": _clean_text(race.get("weather")),
                    "wind_direction": _clean_text(race.get("wind_direction")),
                    "wind_speed": _clean_float(race.get("wind_speed")),
                    "wave_height": _clean_float(race.get("wave_height")),
                    "water_temperature": _clean_float(race.get("water_temperature")),
                    "air_temperature": _clean_float(race.get("air_temperature")),
                    "vote_close_time": _clean_text(race.get("vote_close_time")),
                    "race_start_time": _clean_text(race.get("race_start_time")),
                    "tournament_name": _clean_text(race.get("tournament_name")),
                    "tournament_day": _clean_int(race.get("tournament_day")),
                    "source_types": {source_type},
                    "raw_json": _json_dumps(race),
                    "fetched_at": fetched_at,
                },
            )

        for entry in payload.get("race_entries", []):
            venue_code = _clean_text(entry.get("venue_code"))
            race_number = _clean_int(entry.get("race_number"))
            boat_number = _clean_int(entry.get("boat_number"))
            if not venue_code or race_number is None or boat_number is None:
                continue
            key = (target_date, venue_code, race_number, boat_number)
            base = entry_map.setdefault(
                key,
                {
                    "race_date": target_date,
                    "venue_code": venue_code,
                    "venue_name": venue_map.get(venue_code, {}).get("venue_name", VENUE_CODE_TO_NAME.get(venue_code, venue_code)),
                    "race_number": race_number,
                    "boat_number": boat_number,
                    "source_types": set(),
                    "fetched_at": fetched_at,
                },
            )
            merge_non_empty(
                base,
                {
                    "venue_name": venue_map.get(venue_code, {}).get("venue_name"),
                    "racer_number": _clean_int(entry.get("racer_number")),
                    "racer_name": _clean_text(entry.get("racer_name")),
                    "age": _clean_int(entry.get("age")),
                    "weight": _clean_float(entry.get("weight")),
                    "branch": _clean_text(entry.get("branch")),
                    "racer_class": _clean_text(entry.get("racer_class")),
                    "motor_number": _clean_int(entry.get("motor_number")),
                    "boat_equipment_number": _clean_int(
                        entry.get("boat_equipment_number") or entry.get("boat_number")
                    ),
                    "boat_part": _clean_int(entry.get("boat_part")),
                    "national_win_rate": _clean_float(entry.get("national_win_rate")),
                    "national_quinella_rate": _clean_float(
                        entry.get("national_quinella_rate") or entry.get("national_place_rate")
                    ),
                    "local_win_rate": _clean_float(entry.get("local_win_rate")),
                    "local_quinella_rate": _clean_float(
                        entry.get("local_quinella_rate") or entry.get("local_place_rate")
                    ),
                    "motor_quinella_rate": _clean_float(entry.get("motor_quinella_rate") or entry.get("motor_place_rate")),
                    "boat_quinella_rate": _clean_float(entry.get("boat_quinella_rate") or entry.get("boat_place_rate")),
                    "recent_results": _clean_text(entry.get("recent_results")),
                    "exhibition_time": _clean_float(entry.get("exhibition_time")),
                    "st_timing": _clean_float(entry.get("st_timing")),
                    "tilt_angle": _clean_float(entry.get("tilt_angle")),
                    "result_position": _clean_int(entry.get("result_position")),
                    "result_time": _clean_text(entry.get("result_time")),
                    "disqualified": bool(entry.get("disqualified", False)),
                    "source_types": {source_type},
                    "raw_json": _json_dumps(entry),
                    "fetched_at": fetched_at,
                },
            )

            racer_number = base.get("racer_number")
            if racer_number is not None:
                existing_racer = racer_map.setdefault(
                    racer_number,
                    {
                        "racer_number": racer_number,
                        "racer_name": None,
                        "branch": None,
                        "racer_class": None,
                        "last_race_date": target_date,
                        "last_seen_at": fetched_at,
                        "metadata_json": _json_dumps({}),
                    },
                )
                merge_non_empty(
                    existing_racer,
                    {
                        "racer_name": base.get("racer_name"),
                        "branch": base.get("branch"),
                        "racer_class": base.get("racer_class"),
                    },
                )
                existing_racer["last_race_date"] = target_date
                existing_racer["last_seen_at"] = fetched_at

        for odds in payload.get("odds_data", []):
            venue_code = _clean_text(odds.get("venue_code"))
            ticket_type = _clean_text(odds.get("ticket_type"))
            combination = _clean_text(odds.get("combination"))
            if not venue_code or not ticket_type or not combination:
                continue
            race_number = _clean_int(odds.get("race_number")) or 0
            key = (target_date, venue_code, race_number, ticket_type, combination)
            base = odds_map.setdefault(
                key,
                {
                    "race_date": target_date,
                    "venue_code": venue_code,
                    "race_number": race_number,
                    "ticket_type": ticket_type,
                    "combination": combination,
                    "source_types": set(),
                    "fetched_at": fetched_at,
                },
            )
            merge_non_empty(
                base,
                {
                    "odds": _clean_float(odds.get("odds")),
                    "payout": _clean_int(odds.get("payout")),
                    "source_types": {source_type},
                    "raw_json": _json_dumps(odds),
                    "fetched_at": fetched_at,
                },
            )

        for race in race_map.values():
            race["source_types_json"] = _json_dumps(sorted(race.pop("source_types")))
        for entry in entry_map.values():
            entry["source_types_json"] = _json_dumps(sorted(entry.pop("source_types")))
        for odds in odds_map.values():
            odds["source_types_json"] = _json_dumps(sorted(odds.pop("source_types")))

        return {
            "venues": sorted(venue_map.values(), key=lambda row: row["venue_code"]),
            "racers": sorted(racer_map.values(), key=lambda row: row["racer_number"]),
            "races": sorted(race_map.values(), key=lambda row: (row["race_date"], row["venue_code"], row["race_number"])),
            "race_entries": sorted(
                entry_map.values(),
                key=lambda row: (row["race_date"], row["venue_code"], row["race_number"], row["boat_number"]),
            ),
            "odds_data": sorted(
                odds_map.values(),
                key=lambda row: (row["race_date"], row["venue_code"], row["race_number"], row["ticket_type"], row["combination"]),
            ),
        }

    def _merge_source_payloads(
        self,
        target_date: date,
        schedule_data: Dict[str, Any],
        performance_data: Dict[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        fetched_at = _now()
        venue_map: Dict[str, Dict[str, Any]] = {}
        racer_map: Dict[int, Dict[str, Any]] = {}
        race_map: Dict[tuple[date, str, int], Dict[str, Any]] = {}
        entry_map: Dict[tuple[date, str, int, int], Dict[str, Any]] = {}
        odds_map: Dict[tuple[date, str, int, str, str], Dict[str, Any]] = {}

        def merge_non_empty(existing: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
            for key, value in updates.items():
                if key == "source_types":
                    existing.setdefault("source_types", set()).update(value)
                    continue
                if value not in (None, "", []):
                    existing[key] = value
            return existing

        def ingest(source_type: str, payload: Dict[str, Any]) -> None:
            if not payload:
                return

            for venue in payload.get("venues", []):
                venue_code = _clean_text(venue.get("code"))
                if not venue_code:
                    continue
                venue_map[venue_code] = {
                    "venue_code": venue_code,
                    "venue_name": _clean_text(venue.get("name")) or VENUE_CODE_TO_NAME.get(venue_code, venue_code),
                    "updated_at": fetched_at,
                }

            for racer in payload.get("racers", []):
                racer_number = _clean_int(racer.get("racer_number"))
                if racer_number is None:
                    continue
                existing = racer_map.setdefault(
                    racer_number,
                    {
                        "racer_number": racer_number,
                        "racer_name": None,
                        "branch": None,
                        "racer_class": None,
                        "last_race_date": target_date,
                        "last_seen_at": fetched_at,
                        "metadata_json": _json_dumps({}),
                    },
                )
                merged = merge_non_empty(
                    existing,
                    {
                        "racer_name": _clean_text(racer.get("name")),
                        "branch": _clean_text(racer.get("branch")),
                        "racer_class": _clean_text(racer.get("racer_class")),
                    },
                )
                merged["last_race_date"] = target_date
                merged["last_seen_at"] = fetched_at
                merged["metadata_json"] = _json_dumps(racer)

            for race in payload.get("races", []):
                venue_code = _clean_text(race.get("venue_code"))
                race_number = _clean_int(race.get("race_number"))
                if not venue_code or race_number is None:
                    continue
                key = (target_date, venue_code, race_number)
                base = race_map.setdefault(
                    key,
                    {
                        "race_date": target_date,
                        "venue_code": venue_code,
                        "venue_name": venue_map.get(venue_code, {}).get("venue_name", VENUE_CODE_TO_NAME.get(venue_code, venue_code)),
                        "race_number": race_number,
                        "source_types": set(),
                        "fetched_at": fetched_at,
                    },
                )
                merge_non_empty(
                    base,
                    {
                        "venue_name": venue_map.get(venue_code, {}).get("venue_name"),
                        "race_name": _clean_text(race.get("race_name")),
                        "grade": _clean_text(race.get("grade")),
                        "distance": _clean_int(race.get("distance")),
                        "weather": _clean_text(race.get("weather")),
                        "wind_direction": _clean_text(race.get("wind_direction")),
                        "wind_speed": _clean_float(race.get("wind_speed")),
                        "wave_height": _clean_float(race.get("wave_height")),
                        "water_temperature": _clean_float(race.get("water_temperature")),
                        "air_temperature": _clean_float(race.get("air_temperature")),
                        "vote_close_time": _clean_text(race.get("vote_close_time")),
                        "race_start_time": _clean_text(race.get("race_start_time")),
                        "tournament_name": _clean_text(race.get("tournament_name")),
                        "tournament_day": _clean_int(race.get("tournament_day")),
                        "source_types": {source_type},
                        "raw_json": _json_dumps(race),
                        "fetched_at": fetched_at,
                    },
                )

            for entry in payload.get("race_entries", []):
                venue_code = _clean_text(entry.get("venue_code"))
                race_number = _clean_int(entry.get("race_number"))
                boat_number = _clean_int(entry.get("boat_number"))
                if not venue_code or race_number is None or boat_number is None:
                    continue
                key = (target_date, venue_code, race_number, boat_number)
                base = entry_map.setdefault(
                    key,
                    {
                        "race_date": target_date,
                        "venue_code": venue_code,
                        "venue_name": venue_map.get(venue_code, {}).get("venue_name", VENUE_CODE_TO_NAME.get(venue_code, venue_code)),
                        "race_number": race_number,
                        "boat_number": boat_number,
                        "source_types": set(),
                        "fetched_at": fetched_at,
                    },
                )
                merge_non_empty(
                    base,
                    {
                        "venue_name": venue_map.get(venue_code, {}).get("venue_name"),
                        "racer_number": _clean_int(entry.get("racer_number")),
                        "racer_name": _clean_text(entry.get("racer_name")),
                        "age": _clean_int(entry.get("age")),
                        "weight": _clean_float(entry.get("weight")),
                        "branch": _clean_text(entry.get("branch")),
                        "racer_class": _clean_text(entry.get("racer_class")),
                        "motor_number": _clean_int(entry.get("motor_number")),
                        "boat_equipment_number": _clean_int(
                            entry.get("boat_equipment_number") or entry.get("boat_number")
                        ),
                        "boat_part": _clean_int(entry.get("boat_part")),
                        "national_win_rate": _clean_float(entry.get("national_win_rate")),
                        "national_quinella_rate": _clean_float(
                            entry.get("national_quinella_rate") or entry.get("national_place_rate")
                        ),
                        "local_win_rate": _clean_float(entry.get("local_win_rate")),
                        "local_quinella_rate": _clean_float(
                            entry.get("local_quinella_rate") or entry.get("local_place_rate")
                        ),
                        "motor_quinella_rate": _clean_float(entry.get("motor_quinella_rate") or entry.get("motor_place_rate")),
                        "boat_quinella_rate": _clean_float(entry.get("boat_quinella_rate") or entry.get("boat_place_rate")),
                        "recent_results": _clean_text(entry.get("recent_results")),
                        "exhibition_time": _clean_float(entry.get("exhibition_time")),
                        "st_timing": _clean_float(entry.get("st_timing")),
                        "tilt_angle": _clean_float(entry.get("tilt_angle")),
                        "result_position": _clean_int(entry.get("result_position")),
                        "result_time": _clean_text(entry.get("result_time")),
                        "disqualified": bool(entry.get("disqualified", False)),
                        "source_types": {source_type},
                        "raw_json": _json_dumps(entry),
                        "fetched_at": fetched_at,
                    },
                )

                racer_number = base.get("racer_number")
                if racer_number is not None:
                    existing_racer = racer_map.setdefault(
                        racer_number,
                        {
                            "racer_number": racer_number,
                            "racer_name": None,
                            "branch": None,
                            "racer_class": None,
                            "last_race_date": target_date,
                            "last_seen_at": fetched_at,
                            "metadata_json": _json_dumps({}),
                        },
                    )
                    merge_non_empty(
                        existing_racer,
                        {
                            "racer_name": base.get("racer_name"),
                            "branch": base.get("branch"),
                            "racer_class": base.get("racer_class"),
                        },
                    )
                    existing_racer["last_race_date"] = target_date
                    existing_racer["last_seen_at"] = fetched_at

            for odds in payload.get("odds_data", []):
                venue_code = _clean_text(odds.get("venue_code"))
                ticket_type = _clean_text(odds.get("ticket_type"))
                combination = _clean_text(odds.get("combination"))
                if not venue_code or not ticket_type or not combination:
                    continue
                race_number = _clean_int(odds.get("race_number")) or 0
                key = (target_date, venue_code, race_number, ticket_type, combination)
                base = odds_map.setdefault(
                    key,
                    {
                        "race_date": target_date,
                        "venue_code": venue_code,
                        "race_number": race_number,
                        "ticket_type": ticket_type,
                        "combination": combination,
                        "source_types": set(),
                        "fetched_at": fetched_at,
                    },
                )
                merge_non_empty(
                    base,
                    {
                        "odds": _clean_float(odds.get("odds")),
                        "payout": _clean_int(odds.get("payout")),
                        "source_types": {source_type},
                        "raw_json": _json_dumps(odds),
                        "fetched_at": fetched_at,
                    },
                )

        ingest("schedule", schedule_data)
        ingest("performance", performance_data)

        for race in race_map.values():
            race["source_types_json"] = _json_dumps(sorted(race.pop("source_types")))
        for entry in entry_map.values():
            entry["source_types_json"] = _json_dumps(sorted(entry.pop("source_types")))
        for odds in odds_map.values():
            odds["source_types_json"] = _json_dumps(sorted(odds.pop("source_types")))

        return {
            "venues": sorted(venue_map.values(), key=lambda row: row["venue_code"]),
            "racers": sorted(racer_map.values(), key=lambda row: row["racer_number"]),
            "races": sorted(race_map.values(), key=lambda row: (row["race_date"], row["venue_code"], row["race_number"])),
            "race_entries": sorted(
                entry_map.values(),
                key=lambda row: (row["race_date"], row["venue_code"], row["race_number"], row["boat_number"]),
            ),
            "odds_data": sorted(
                odds_map.values(),
                key=lambda row: (row["race_date"], row["venue_code"], row["race_number"], row["ticket_type"], row["combination"]),
            ),
        }

    def start_fetch_run(
        self,
        source: str,
        start_date: Optional[date],
        end_date: Optional[date],
        command: Iterable[str],
        parameters: Optional[Dict[str, Any]] = None,
        note: Optional[str] = None,
        status: str = "running",
    ) -> str:
        run_id = uuid.uuid4().hex
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO fetch_runs (
                    id, source, start_date, end_date, command_json, parameters_json,
                    artifacts_json, status, note, created_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source,
                    start_date,
                    end_date,
                    _json_dumps(list(command)),
                    _json_dumps(parameters or {}),
                    _json_dumps([]),
                    status,
                    note,
                    _now(),
                    None,
                ),
            )
        return run_id

    def finish_fetch_run(
        self,
        run_id: str,
        status: str,
        artifacts: Optional[List[Dict[str, Any]]] = None,
        note: Optional[str] = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE fetch_runs
                SET status = ?, artifacts_json = ?, note = ?, completed_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    _json_dumps(artifacts or []),
                    note,
                    _now(),
                    run_id,
                ),
            )

    def register_model(
        self,
        model_type: str,
        model_path: str,
        training_start_date: Optional[date],
        training_end_date: Optional[date],
        feature_count: int = 0,
        training_samples: int = 0,
        validation_scores: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        activate: bool = True,
    ) -> str:
        model_id = uuid.uuid4().hex
        with self.connect() as conn:
            if activate:
                conn.execute("UPDATE models SET is_active = FALSE WHERE is_active = TRUE")
            conn.execute(
                """
                INSERT INTO models (
                    id, model_type, model_path, training_start_date, training_end_date,
                    feature_count, training_samples, validation_scores_json, metadata_json,
                    is_active, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model_id,
                    model_type,
                    model_path,
                    training_start_date,
                    training_end_date,
                    feature_count,
                    training_samples,
                    _json_dumps(validation_scores or {}),
                    _json_dumps(metadata or {}),
                    activate,
                    _now(),
                ),
            )
        return model_id

    def activate_model(self, model_id: str) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE models SET is_active = FALSE WHERE is_active = TRUE")
            conn.execute("UPDATE models SET is_active = TRUE WHERE id = ?", (model_id,))

    def get_active_model(self) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM models
                WHERE is_active = TRUE
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            rows = self._rows_to_dicts(cursor)
        return self._decode_model(rows[0]) if rows else None

    def list_models(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM models
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = self._rows_to_dicts(cursor)
        return [self._decode_model(row) for row in rows]

    def start_prediction_run(
        self,
        target_date: date,
        model_id: Optional[str],
        model_path: Optional[str],
        requested_races: Optional[List[int]] = None,
        note: Optional[str] = None,
    ) -> str:
        run_id = uuid.uuid4().hex
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO prediction_runs (
                    id, target_date, model_id, model_path, requested_races_json,
                    total_races, successful_races, failed_races, output_path,
                    summary_json, status, note, created_at, completed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    target_date,
                    model_id,
                    model_path,
                    _json_dumps(requested_races or []),
                    0,
                    0,
                    0,
                    None,
                    _json_dumps({}),
                    "running",
                    note,
                    _now(),
                    None,
                ),
            )
        return run_id

    def save_prediction_run_results(
        self,
        prediction_run_id: str,
        predictions: List[Any],
        output_path: Optional[str] = None,
        status: str = "completed",
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        created_at = _now()
        inserted = 0

        with self.connect() as conn:
            conn.execute("DELETE FROM ticket_predictions WHERE prediction_run_id = ?", (prediction_run_id,))
            conn.execute("DELETE FROM race_predictions WHERE prediction_run_id = ?", (prediction_run_id,))

            for prediction in predictions:
                self._insert_prediction(conn, prediction_run_id, prediction, created_at)
                inserted += 1

            summary = {
                "total_races": inserted,
                "successful_races": inserted,
                "failed_races": 0,
                "ticket_predictions": self._count_ticket_predictions(conn, prediction_run_id),
            }

            conn.execute(
                """
                UPDATE prediction_runs
                SET total_races = ?,
                    successful_races = ?,
                    failed_races = ?,
                    output_path = ?,
                    summary_json = ?,
                    status = ?,
                    note = ?,
                    completed_at = ?
                WHERE id = ?
                """,
                (
                    summary["total_races"],
                    summary["successful_races"],
                    summary["failed_races"],
                    output_path,
                    _json_dumps(summary),
                    status,
                    note,
                    _now(),
                    prediction_run_id,
                ),
            )

        return summary

    def _count_ticket_predictions(
        self,
        conn: duckdb.DuckDBPyConnection,
        prediction_run_id: str,
    ) -> int:
        row = conn.execute(
            "SELECT COUNT(*) FROM ticket_predictions WHERE prediction_run_id = ?",
            (prediction_run_id,),
        ).fetchone()
        return int(row[0]) if row else 0

    def _insert_prediction(
        self,
        conn: duckdb.DuckDBPyConnection,
        prediction_run_id: str,
        prediction: Any,
        created_at: datetime,
    ) -> None:
        race_prediction_id = uuid.uuid4().hex
        racer_predictions = getattr(prediction, "racer_predictions", {}) or {}
        venue_code = str(getattr(prediction, "venue_code", "") or "")
        venue_name = VENUE_CODE_TO_NAME.get(venue_code, venue_code)
        sorted_racers = sorted(
            racer_predictions.items(),
            key=lambda item: float((item[1] or {}).get("1位", 0.0)),
            reverse=True,
        )
        top3 = [
            {
                "racer_id": int(racer_id),
                "win_probability": float((probs or {}).get("1位", 0.0)),
            }
            for racer_id, probs in sorted_racers[:3]
        ]
        top_pick_racer_id = top3[0]["racer_id"] if top3 else None
        top_pick_probability = top3[0]["win_probability"] if top3 else None

        conn.execute(
            """
            INSERT INTO race_predictions (
                id, prediction_run_id, race_id, target_date, venue_code, venue_name,
                race_number, confidence_score, top_pick_racer_id, top_pick_probability,
                top3_json, racer_predictions_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                race_prediction_id,
                prediction_run_id,
                int(getattr(prediction, "race_id", 0)),
                getattr(prediction, "race_date"),
                venue_code,
                venue_name,
                int(getattr(prediction, "race_number", 0)),
                float(getattr(prediction, "confidence_score", 0.0)),
                top_pick_racer_id,
                top_pick_probability,
                _json_dumps(top3),
                _json_dumps(racer_predictions),
                created_at,
            ),
        )

        ticket_probabilities = getattr(prediction, "ticket_probabilities", {}) or {}
        for ticket_type, combinations in ticket_probabilities.items():
            sorted_combinations = sorted(
                (combinations or {}).items(),
                key=lambda item: float(item[1]),
                reverse=True,
            )[:5]
            for index, (combination, probability) in enumerate(sorted_combinations, start=1):
                conn.execute(
                    """
                    INSERT INTO ticket_predictions (
                        id, race_prediction_id, prediction_run_id, ticket_type,
                        combination, probability, rank_order, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        race_prediction_id,
                        prediction_run_id,
                        str(ticket_type),
                        str(combination),
                        float(probability),
                        index,
                        created_at,
                    ),
                )

    def get_latest_prediction_run(self) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM prediction_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            rows = self._rows_to_dicts(cursor)
        return self._decode_prediction_run(rows[0]) if rows else None

    def get_prediction_run_details(self, prediction_run_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            run_rows = self._rows_to_dicts(
                conn.execute("SELECT * FROM prediction_runs WHERE id = ?", (prediction_run_id,))
            )
            if not run_rows:
                return None

            race_rows = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT *
                    FROM race_predictions
                    WHERE prediction_run_id = ?
                    ORDER BY venue_code, race_number
                    """,
                    (prediction_run_id,),
                )
            )
            ticket_rows = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT *
                    FROM ticket_predictions
                    WHERE prediction_run_id = ?
                    ORDER BY race_prediction_id, ticket_type, rank_order
                    """,
                    (prediction_run_id,),
                )
            )

        run = self._decode_prediction_run(run_rows[0])
        tickets_by_race: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for row in ticket_rows:
            race_prediction_id = row["race_prediction_id"]
            tickets_by_race.setdefault(race_prediction_id, {}).setdefault(row["ticket_type"], []).append(
                {
                    "combination": row["combination"],
                    "probability": row["probability"],
                    "rank_order": row["rank_order"],
                }
            )

        races = []
        for row in race_rows:
            row["top3"] = self._decode_json(row.pop("top3_json")) or []
            row["racer_predictions"] = self._decode_json(row.pop("racer_predictions_json")) or {}
            row["ticket_predictions"] = tickets_by_race.get(row["id"], {})
            races.append(row)

        run["races"] = races
        return run

    def get_predictions_for_date(self, target_date: date) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            rows = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT id
                    FROM prediction_runs
                    WHERE target_date = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (target_date,),
                )
            )
        if not rows:
            return None
        return self.get_prediction_run_details(rows[0]["id"])

    def get_race_prediction(
        self,
        target_date: date,
        venue_code: str,
        race_number: int,
    ) -> Optional[Dict[str, Any]]:
        run = self.get_predictions_for_date(target_date)
        if not run:
            return None
        for race in run.get("races", []):
            if race["venue_code"] == venue_code and int(race["race_number"]) == int(race_number):
                race["prediction_run_id"] = run["id"]
                race["target_date"] = run["target_date"]
                race["model_id"] = run.get("model_id")
                race["model_path"] = run.get("model_path")
                return race
        return None

    def get_status_summary(self) -> Dict[str, Any]:
        with self.connect() as conn:
            fetch_count = conn.execute("SELECT COUNT(*) FROM fetch_runs").fetchone()[0]
            model_count = conn.execute("SELECT COUNT(*) FROM models").fetchone()[0]
            prediction_count = conn.execute("SELECT COUNT(*) FROM prediction_runs").fetchone()[0]
            races_count = conn.execute("SELECT COUNT(*) FROM races_prerace").fetchone()[0]
            entries_count = conn.execute("SELECT COUNT(*) FROM race_entries_prerace").fetchone()[0]
            results_count = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
            odds_count = conn.execute("SELECT COUNT(*) FROM odds_data").fetchone()[0]
            merged_races_count = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
            merged_entries_count = conn.execute("SELECT COUNT(*) FROM race_entries").fetchone()[0]
            source_dates = conn.execute(
                "SELECT MIN(race_date), MAX(race_date), COUNT(DISTINCT race_date) FROM races_prerace"
            ).fetchone()
            latest_fetch = self._rows_to_dicts(
                conn.execute(
                    "SELECT * FROM fetch_runs ORDER BY created_at DESC LIMIT 1"
                )
            )
            latest_prediction = self._rows_to_dicts(
                conn.execute(
                    "SELECT * FROM prediction_runs ORDER BY created_at DESC LIMIT 1"
                )
            )
            analysis_views = self._rows_to_dicts(
                conn.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_type = 'VIEW'
                      AND table_schema = 'main'
                      AND table_name LIKE 'analysis_%'
                    ORDER BY table_name
                    """
                )
            )

        return {
            "db_path": str(self.db_path),
            "fetch_runs": int(fetch_count),
            "models": int(model_count),
            "prediction_runs": int(prediction_count),
            "source_tables": {
                "races_prerace": int(races_count),
                "race_entries_prerace": int(entries_count),
                "race_results": int(results_count),
                "odds_data": int(odds_count),
            },
            "legacy_merged_tables": {
                "races": int(merged_races_count),
                "race_entries": int(merged_entries_count),
            },
            "source_date_range": {
                "start_date": source_dates[0],
                "end_date": source_dates[1],
                "days": int(source_dates[2] or 0),
            },
            "migration_required": bool(
                (merged_races_count or merged_entries_count)
                and not (races_count or entries_count or results_count)
            ),
            "migration_note": (
                "pre-race safe tables are empty; rerun fetch to rebuild DuckDB with schedule/results separation"
                if (merged_races_count or merged_entries_count) and not (races_count or entries_count or results_count)
                else None
            ),
            "active_model": self.get_active_model(),
            "latest_fetch_run": self._decode_fetch_run(latest_fetch[0]) if latest_fetch else None,
            "latest_prediction_run": self._decode_prediction_run(latest_prediction[0]) if latest_prediction else None,
            "analysis_views": [row["table_name"] for row in analysis_views],
        }

    def _decode_model(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["validation_scores"] = self._decode_json(row.pop("validation_scores_json"))
        row["metadata"] = self._decode_json(row.pop("metadata_json"))
        return row

    def _decode_fetch_run(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["command"] = self._decode_json(row.pop("command_json"))
        row["parameters"] = self._decode_json(row.pop("parameters_json"))
        row["artifacts"] = self._decode_json(row.pop("artifacts_json"))
        return row

    def _decode_prediction_run(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["requested_races"] = self._decode_json(row.pop("requested_races_json"))
        row["summary"] = self._decode_json(row.pop("summary_json"))
        return row
