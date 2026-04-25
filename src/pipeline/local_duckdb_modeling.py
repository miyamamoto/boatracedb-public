from __future__ import annotations

import math
import pickle
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score


CLASS_CODE_MAP = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}
GRADE_CODE_MAP = {
    "一般": 1,
    "G3": 2,
    "G2": 3,
    "G1": 4,
    "SG": 5,
    "PG1": 6,
}

FEATURE_GROUPS: Dict[str, List[str]] = {
    "entry_static": [
        "boat_number",
        "age",
        "weight",
        "motor_number",
        "boat_equipment_number",
        "national_win_rate",
        "national_quinella_rate",
        "local_win_rate",
        "local_quinella_rate",
        "motor_quinella_rate",
        "boat_quinella_rate",
        "venue_code_int",
        "class_code",
    ],
    "race_context": [
        "grade_code",
        "distance",
        "wind_direction_code",
        "wind_speed",
        "wave_height",
        "water_temperature",
        "air_temperature",
    ],
    "racer_history": [
        "racer_count",
        "racer_win_rate",
        "racer_top3_rate",
        "racer_avg_finish",
    ],
    "racer_venue_history": [
        "racer_venue_count",
        "racer_venue_win_rate",
        "racer_venue_top3_rate",
        "racer_venue_avg_finish",
    ],
    "motor_history": [
        "motor_count",
        "motor_win_rate",
        "motor_top3_rate",
        "motor_avg_finish",
    ],
    "lane_history": [
        "lane_count",
        "lane_win_rate",
        "lane_top3_rate",
        "lane_avg_finish",
    ],
    "race_relative": [
        "lane_win_rate_race_diff",
        "lane_win_rate_race_rank",
        "lane_win_rate_race_gap_to_best",
        "lane_top3_rate_race_diff",
        "lane_top3_rate_race_gap_to_best",
        "national_quinella_rate_race_diff",
        "national_quinella_rate_race_gap_to_best",
        "motor_quinella_rate_race_diff",
        "motor_quinella_rate_race_gap_to_best",
        "motor_top3_rate_race_diff",
        "racer_top3_rate_race_diff",
        "national_win_rate_race_diff",
        "national_win_rate_race_gap_to_best",
        "local_quinella_rate_race_diff",
    ],
}
TRAINING_FEATURE_EXCLUSIONS = {
    "distance",
    "wind_direction_code",
    "wind_speed",
    "wave_height",
    "water_temperature",
    "air_temperature",
}
TRAINING_GROUP_EXCLUSIONS = {"racer_venue_history"}
FEATURE_COLUMNS = [
    feature
    for group_name in (
        "entry_static",
        "race_context",
        "racer_history",
        "motor_history",
        "lane_history",
        "race_relative",
    )
    for feature in FEATURE_GROUPS[group_name]
    if group_name not in TRAINING_GROUP_EXCLUSIONS
    and feature not in TRAINING_FEATURE_EXCLUSIONS
]
FEATURE_TO_GROUP = {
    feature: group_name
    for group_name, features in FEATURE_GROUPS.items()
    for feature in features
}
FEATURE_STAGE_LABELS = {
    "race_context": "レース条件を結合",
    "racer_history": "選手の通算履歴を集計",
    "racer_venue_history": "選手の会場別履歴を集計",
    "motor_history": "モーター履歴を集計",
    "lane_history": "艇番履歴を集計",
    "race_relative": "レース内相対値を計算",
    "finalize": "特徴量を正規化",
    "complete": "特徴量フレームを確定",
}
RACE_RELATIVE_FEATURE_SPECS: Dict[str, Dict[str, str]] = {
    "lane_win_rate_race_diff": {"source": "lane_win_rate", "kind": "diff"},
    "lane_win_rate_race_rank": {"source": "lane_win_rate", "kind": "rank"},
    "lane_win_rate_race_gap_to_best": {"source": "lane_win_rate", "kind": "gap"},
    "lane_top3_rate_race_diff": {"source": "lane_top3_rate", "kind": "diff"},
    "lane_top3_rate_race_gap_to_best": {"source": "lane_top3_rate", "kind": "gap"},
    "national_quinella_rate_race_diff": {"source": "national_quinella_rate", "kind": "diff"},
    "national_quinella_rate_race_gap_to_best": {"source": "national_quinella_rate", "kind": "gap"},
    "motor_quinella_rate_race_diff": {"source": "motor_quinella_rate", "kind": "diff"},
    "motor_quinella_rate_race_gap_to_best": {"source": "motor_quinella_rate", "kind": "gap"},
    "motor_top3_rate_race_diff": {"source": "motor_top3_rate", "kind": "diff"},
    "racer_top3_rate_race_diff": {"source": "racer_top3_rate", "kind": "diff"},
    "national_win_rate_race_diff": {"source": "national_win_rate", "kind": "diff"},
    "national_win_rate_race_gap_to_best": {"source": "national_win_rate", "kind": "gap"},
    "local_quinella_rate_race_diff": {"source": "local_quinella_rate", "kind": "diff"},
}
ZERO_IMPORTANCE_SPLIT_THRESHOLD = 0
NEAR_ZERO_SPLIT_THRESHOLD = 100
NEAR_ZERO_GAIN_SHARE_THRESHOLD = 0.01
MIN_RETAINED_FEATURES = 12
PRUNE_LOGLOSS_TOLERANCE = 0.01
PRUNE_AUC_TOLERANCE = 0.02
PRUNE_HIT_RATE_TOLERANCE = 0.02
SLOW_STAGE_SHARE_THRESHOLD = 0.10
MIN_STAGE_SECONDS_FOR_COST_AUDIT = 0.005
LOW_VALUE_GROUP_GAIN_SHARE_THRESHOLD = 0.01
RETIRED_FEATURES = {
    "distance": "全件ほぼ定数で、分割重要度がほぼゼロだったため新規学習から除外",
    "wind_direction_code": "ローカル経路で有効な入力が揃わず、重要度がゼロだったため新規学習から除外",
    "wind_speed": "ローカル経路で有効な入力が揃わず、重要度がゼロだったため新規学習から除外",
    "wave_height": "ローカル経路で有効な入力が揃わず、重要度がゼロだったため新規学習から除外",
    "water_temperature": "ローカル経路で有効な入力が揃わず、重要度がゼロだったため新規学習から除外",
    "air_temperature": "ローカル経路で有効な入力が揃わず、重要度がゼロだったため新規学習から除外",
}
RETIRED_GROUPS = {
    "racer_venue_history": "会場別履歴は計算コストが相対的に高い一方で、直近監査では寄与が低かったため新規学習から除外",
}


@dataclass
class LocalRacePrediction:
    race_id: int
    race_date: date
    venue_code: str
    race_number: int
    racer_predictions: Dict[int, Dict[str, Any]]
    ticket_probabilities: Dict[str, Dict[str, float]]
    confidence_score: float
    prediction_timestamp: datetime


ProgressCallback = Callable[[str, Dict[str, Any]], None]


def _emit_progress(
    progress_callback: Optional[ProgressCallback],
    event: str,
    **payload: Any,
) -> None:
    if progress_callback is not None:
        progress_callback(event, payload)


def _compute_history_metrics(
    frame: pd.DataFrame,
    group_columns: List[str],
    prefix: str,
) -> pd.DataFrame:
    daily = (
        frame.groupby(group_columns + ["race_date"], dropna=False)
        .agg(
            daily_races=("boat_number", "size"),
            daily_wins=("is_winner", "sum"),
            daily_top3=("is_top3", "sum"),
            daily_finish_sum=("finish_numeric", "sum"),
        )
        .reset_index()
        .sort_values(group_columns + ["race_date"])
    )

    grouped = daily.groupby(group_columns, dropna=False)
    daily[f"{prefix}_count"] = grouped["daily_races"].cumsum() - daily["daily_races"]
    daily[f"{prefix}_wins"] = grouped["daily_wins"].cumsum() - daily["daily_wins"]
    daily[f"{prefix}_top3"] = grouped["daily_top3"].cumsum() - daily["daily_top3"]
    daily[f"{prefix}_finish_sum"] = grouped["daily_finish_sum"].cumsum() - daily["daily_finish_sum"]

    count_series = daily[f"{prefix}_count"].replace(0, np.nan)
    daily[f"{prefix}_win_rate"] = (daily[f"{prefix}_wins"] / count_series).fillna(0.0)
    daily[f"{prefix}_top3_rate"] = (daily[f"{prefix}_top3"] / count_series).fillna(0.0)
    daily[f"{prefix}_avg_finish"] = (daily[f"{prefix}_finish_sum"] / count_series).fillna(0.0)

    return frame.merge(
        daily[
            group_columns
            + [
                "race_date",
                f"{prefix}_count",
                f"{prefix}_win_rate",
                f"{prefix}_top3_rate",
                f"{prefix}_avg_finish",
            ]
        ],
        on=group_columns + ["race_date"],
        how="left",
    )


def _compute_race_relative_features(
    frame: pd.DataFrame,
    selected_feature_columns: List[str],
) -> pd.DataFrame:
    """Add pre-race features that compare each boat against same-race rivals."""

    race_keys = ["race_date", "venue_code", "race_number"]
    grouped_keys = [frame[key] for key in race_keys]
    computed_sources: Dict[str, Dict[str, pd.Series]] = {}

    for feature_name in selected_feature_columns:
        spec = RACE_RELATIVE_FEATURE_SPECS.get(feature_name)
        if not spec:
            continue

        source_column = spec["source"]
        if source_column not in frame.columns:
            frame[feature_name] = 0.0
            continue

        if source_column not in computed_sources:
            values = pd.to_numeric(frame[source_column], errors="coerce").fillna(0.0)
            grouped = values.groupby(grouped_keys)
            computed_sources[source_column] = {
                "diff": values - grouped.transform("mean"),
                "gap": grouped.transform("max") - values,
                "rank": grouped.rank(method="average", ascending=False),
            }

        frame[feature_name] = (
            pd.to_numeric(computed_sources[source_column][spec["kind"]], errors="coerce")
            .fillna(0.0)
            .astype(float)
        )

    return frame


def _resolve_feature_columns(feature_columns: Optional[List[str]]) -> List[str]:
    if feature_columns is None:
        return list(FEATURE_COLUMNS)

    resolved = list(dict.fromkeys(feature_columns))
    invalid = [column for column in resolved if column not in FEATURE_TO_GROUP]
    if invalid:
        raise ValueError(f"Unknown feature columns: {', '.join(sorted(invalid))}")
    return resolved


def _selected_feature_groups(feature_columns: List[str]) -> List[str]:
    groups = {FEATURE_TO_GROUP[column] for column in feature_columns}
    ordered_groups = [
        group_name
        for group_name in (
            "race_context",
            "racer_history",
            "racer_venue_history",
            "motor_history",
            "lane_history",
            "race_relative",
        )
        if group_name in groups
    ]
    return ordered_groups


def build_modeling_frame(
    entries_df: pd.DataFrame,
    races_df: pd.DataFrame,
    progress_callback: Optional[ProgressCallback] = None,
    event_prefix: str = "modeling",
    feature_columns: Optional[List[str]] = None,
    return_profile: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, Dict[str, Any]]:
    selected_feature_columns = _resolve_feature_columns(feature_columns)
    selected_groups = _selected_feature_groups(selected_feature_columns)
    profile = {
        "selected_feature_columns": selected_feature_columns,
        "selected_groups": selected_groups,
        "stage_timings": [],
        "total_elapsed_seconds": 0.0,
    }

    if entries_df.empty:
        if return_profile:
            return pd.DataFrame(), profile
        return pd.DataFrame()

    frame = entries_df.copy()
    races = races_df.copy()
    stage_order = selected_groups + ["finalize", "complete"]
    total_steps = len(stage_order)
    current_step = 0
    build_started_at = time.perf_counter()

    def announce(stage_name: str) -> float:
        nonlocal current_step
        current_step += 1
        label = FEATURE_STAGE_LABELS[stage_name]
        _emit_progress(
            progress_callback,
            f"{event_prefix}:feature_stage",
            step=current_step,
            total=total_steps,
            label=label,
        )
        return time.perf_counter()

    def finish_stage(stage_name: str, started_at: float) -> None:
        profile["stage_timings"].append(
            {
                "stage": stage_name,
                "label": FEATURE_STAGE_LABELS[stage_name],
                "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                "feature_count": len(FEATURE_GROUPS.get(stage_name, [])),
            }
        )

    frame["race_date"] = pd.to_datetime(frame["race_date"])
    races["race_date"] = pd.to_datetime(races["race_date"])

    if "race_context" in selected_groups:
        stage_started_at = announce("race_context")
        race_columns = ["race_date", "venue_code", "race_number"]
        if "grade_code" in selected_feature_columns:
            race_columns.append("grade")
        if "distance" in selected_feature_columns:
            race_columns.append("distance")
        if "wind_direction_code" in selected_feature_columns:
            race_columns.append("wind_direction")
        for column in ("wind_speed", "wave_height", "water_temperature", "air_temperature"):
            if column in selected_feature_columns:
                race_columns.append(column)
        available_race_columns = [column for column in dict.fromkeys(race_columns) if column in races.columns]
        frame = frame.merge(
            races[available_race_columns].drop_duplicates(),
            on=["race_date", "venue_code", "race_number"],
            how="left",
        )
        finish_stage("race_context", stage_started_at)

    numeric_columns = [
        column
        for column in (
            "boat_number",
            "age",
            "weight",
            "motor_number",
            "boat_equipment_number",
            "national_win_rate",
            "national_quinella_rate",
            "local_win_rate",
            "local_quinella_rate",
            "motor_quinella_rate",
            "boat_quinella_rate",
            "distance",
            "wind_speed",
            "wave_height",
            "water_temperature",
            "air_temperature",
            "result_position",
        )
        if column == "result_position" or column in selected_feature_columns
    ]
    for column in numeric_columns:
        if column not in frame.columns:
            frame[column] = 0.0
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    racer_class_series = (
        frame["racer_class"] if "racer_class" in frame.columns else pd.Series("", index=frame.index)
    )
    grade_series = frame["grade"] if "grade" in frame.columns else pd.Series("", index=frame.index)

    frame["venue_code_int"] = pd.to_numeric(frame["venue_code"], errors="coerce").fillna(0.0)
    frame["class_code"] = racer_class_series.map(CLASS_CODE_MAP).fillna(0.0)
    frame["grade_code"] = grade_series.map(GRADE_CODE_MAP).fillna(0.0)
    if "wind_direction_code" in selected_feature_columns:
        wind_direction_series = (
            frame["wind_direction"] if "wind_direction" in frame.columns else pd.Series(0, index=frame.index)
        )
        frame["wind_direction_code"] = pd.to_numeric(wind_direction_series, errors="coerce").fillna(0.0)

    frame["finish_numeric"] = frame["result_position"].fillna(7.0)
    frame["is_winner"] = (frame["result_position"] == 1).fillna(False).astype(int)
    frame["is_top3"] = (
        frame["result_position"].notna() & (frame["result_position"] <= 3)
    ).fillna(False).astype(int)

    for key_column, fill_value in {
        "racer_number": -1,
        "motor_number": -1,
        "boat_number": -1,
        "venue_code": "00",
    }.items():
        frame[key_column] = frame[key_column].fillna(fill_value)

    frame = frame.sort_values(["race_date", "venue_code", "race_number", "boat_number"]).reset_index(drop=True)

    if "racer_history" in selected_groups:
        stage_started_at = announce("racer_history")
        frame = _compute_history_metrics(frame, ["racer_number"], "racer")
        finish_stage("racer_history", stage_started_at)
    if "racer_venue_history" in selected_groups:
        stage_started_at = announce("racer_venue_history")
        frame = _compute_history_metrics(frame, ["racer_number", "venue_code"], "racer_venue")
        finish_stage("racer_venue_history", stage_started_at)
    if "motor_history" in selected_groups:
        stage_started_at = announce("motor_history")
        frame = _compute_history_metrics(frame, ["venue_code", "motor_number"], "motor")
        finish_stage("motor_history", stage_started_at)
    if "lane_history" in selected_groups:
        stage_started_at = announce("lane_history")
        frame = _compute_history_metrics(frame, ["boat_number"], "lane")
        finish_stage("lane_history", stage_started_at)
    if "race_relative" in selected_groups:
        stage_started_at = announce("race_relative")
        frame = _compute_race_relative_features(frame, selected_feature_columns)
        finish_stage("race_relative", stage_started_at)

    stage_started_at = announce("finalize")
    for column in selected_feature_columns:
        if column not in frame.columns:
            frame[column] = 0.0
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    finish_stage("finalize", stage_started_at)

    stage_started_at = announce("complete")
    finish_stage("complete", stage_started_at)
    profile["total_elapsed_seconds"] = round(time.perf_counter() - build_started_at, 6)

    if return_profile:
        return frame, profile
    return frame


def _evaluate_race_hit_rate(frame: pd.DataFrame, probabilities: np.ndarray) -> float:
    scored = frame[["race_date", "venue_code", "race_number", "is_winner"]].copy()
    scored["probability"] = probabilities
    top_rows = scored.groupby(["race_date", "venue_code", "race_number"])["probability"].idxmax()
    winners = scored.loc[top_rows, "is_winner"]
    if winners.empty:
        return 0.0
    return float(winners.mean())


def _fit_and_evaluate_model(
    train_frame: pd.DataFrame,
    valid_frame: pd.DataFrame,
    feature_columns: List[str],
    progress_callback: Optional[ProgressCallback] = None,
    fit_event: str = "train:fit_model",
) -> tuple[lgb.LGBMClassifier, Dict[str, Any]]:
    if not feature_columns:
        raise ValueError("学習に使う特徴量がありません")

    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=2,
        verbosity=-1,
    )

    x_train = train_frame[feature_columns]
    y_train = train_frame["is_winner"]
    x_valid = valid_frame[feature_columns]
    y_valid = valid_frame["is_winner"]

    _emit_progress(progress_callback, fit_event, status="running", feature_count=len(feature_columns))
    model.fit(
        x_train,
        y_train,
        eval_set=[(x_valid, y_valid)],
        eval_metric="binary_logloss",
        callbacks=[lgb.log_evaluation(period=0)],
    )

    valid_probabilities = model.predict_proba(x_valid)[:, 1]
    validation_scores: Dict[str, Any] = {
        "logloss": float(log_loss(y_valid, valid_probabilities)),
        "race_hit_rate": _evaluate_race_hit_rate(valid_frame, valid_probabilities),
        "positive_rate": float(y_valid.mean()),
    }
    if len(np.unique(y_valid)) > 1:
        validation_scores["auc"] = float(roc_auc_score(y_valid, valid_probabilities))
    return model, validation_scores


def _calculate_feature_metrics(
    model: lgb.LGBMClassifier,
    feature_columns: List[str],
    build_profile: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    booster = model.booster_
    split_importance = np.asarray(model.feature_importances_, dtype=float)
    if booster is not None:
        gain_importance = np.asarray(
            booster.feature_importance(importance_type="gain"),
            dtype=float,
        )
    else:
        gain_importance = np.zeros(len(feature_columns), dtype=float)
    total_gain = float(gain_importance.sum())
    stage_timings = {
        item["stage"]: float(item["elapsed_seconds"])
        for item in (build_profile or {}).get("stage_timings", [])
    }

    metrics: List[Dict[str, Any]] = []
    for feature, split_value, gain_value in zip(feature_columns, split_importance, gain_importance):
        group_name = FEATURE_TO_GROUP[feature]
        metrics.append(
            {
                "feature": feature,
                "group": group_name,
                "importance": int(split_value),
                "split_importance": int(split_value),
                "gain_importance": float(gain_value),
                "gain_share": float(gain_value / total_gain) if total_gain > 0 else 0.0,
                "stage_elapsed_seconds": float(stage_timings.get(group_name, 0.0)),
            }
        )

    return sorted(
        metrics,
        key=lambda item: (item["gain_importance"], item["importance"]),
        reverse=True,
    )


def _top_feature_importance(
    feature_metrics: List[Dict[str, Any]],
    limit: int = 20,
) -> List[Dict[str, Any]]:
    return [
        {
            "feature": metric["feature"],
            "group": metric["group"],
            "importance": metric["importance"],
            "split_importance": metric["split_importance"],
            "gain_importance": round(float(metric["gain_importance"]), 6),
            "gain_share": round(float(metric["gain_share"]), 6),
        }
        for metric in feature_metrics[:limit]
    ]


def _select_feature_plan(feature_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    metric_by_feature = {metric["feature"]: metric for metric in feature_metrics}
    retained_features: List[str] = []
    pruned_features: List[Dict[str, Any]] = []

    for feature in FEATURE_COLUMNS:
        metric = metric_by_feature[feature]
        prune_reason: Optional[str] = None
        if metric["split_importance"] <= ZERO_IMPORTANCE_SPLIT_THRESHOLD:
            prune_reason = "zero_importance"
        elif (
            metric["split_importance"] <= NEAR_ZERO_SPLIT_THRESHOLD
            and metric["gain_share"] <= NEAR_ZERO_GAIN_SHARE_THRESHOLD
        ):
            prune_reason = "near_zero_importance"

        if prune_reason is None:
            retained_features.append(feature)
            continue

        pruned_features.append(
            {
                "feature": feature,
                "group": metric["group"],
                "reason": prune_reason,
                "importance": metric["importance"],
                "gain_share": round(float(metric["gain_share"]), 6),
            }
        )

    if len(retained_features) < min(MIN_RETAINED_FEATURES, len(FEATURE_COLUMNS)):
        retained_set = set(retained_features)
        restore_candidates = [
            metric
            for metric in feature_metrics
            if metric["feature"] not in retained_set
        ]
        restore_candidates.sort(
            key=lambda item: (item["gain_importance"], item["importance"]),
            reverse=True,
        )
        for metric in restore_candidates:
            if len(retained_features) >= min(MIN_RETAINED_FEATURES, len(FEATURE_COLUMNS)):
                break
            retained_features.append(metric["feature"])

        pruned_features = [
            item
            for item in pruned_features
            if item["feature"] not in set(retained_features)
        ]

    return {
        "retained_feature_columns": retained_features,
        "pruned_features": pruned_features,
    }


def _identify_cost_candidates(
    feature_metrics: List[Dict[str, Any]],
    build_profile: Dict[str, Any],
) -> List[Dict[str, Any]]:
    stage_timings = {
        item["stage"]: float(item["elapsed_seconds"])
        for item in build_profile.get("stage_timings", [])
    }
    total_elapsed = float(build_profile.get("total_elapsed_seconds", 0.0))
    metrics_by_group: Dict[str, List[Dict[str, Any]]] = {}
    for metric in feature_metrics:
        metrics_by_group.setdefault(metric["group"], []).append(metric)

    candidates: List[Dict[str, Any]] = []
    for group_name, metrics in metrics_by_group.items():
        stage_seconds = float(stage_timings.get(group_name, 0.0))
        stage_share = float(stage_seconds / total_elapsed) if total_elapsed > 0 else 0.0
        if (
            stage_seconds < MIN_STAGE_SECONDS_FOR_COST_AUDIT
            or stage_share < SLOW_STAGE_SHARE_THRESHOLD
        ):
            continue

        group_gain_share = sum(float(metric["gain_share"]) for metric in metrics)
        if group_gain_share > LOW_VALUE_GROUP_GAIN_SHARE_THRESHOLD:
            continue

        candidates.append(
            {
                "group": group_name,
                "stage_elapsed_seconds": round(stage_seconds, 6),
                "stage_share": round(stage_share, 6),
                "group_gain_share": round(group_gain_share, 6),
                "feature_count": len(metrics),
                "reason": "slow_and_low_value_group",
            }
        )

    return sorted(candidates, key=lambda item: item["stage_elapsed_seconds"], reverse=True)


def _accept_pruned_model(
    base_scores: Dict[str, Any],
    candidate_scores: Dict[str, Any],
) -> bool:
    if candidate_scores["logloss"] > base_scores["logloss"] + PRUNE_LOGLOSS_TOLERANCE:
        return False

    if (
        "auc" in base_scores
        and "auc" in candidate_scores
        and candidate_scores["auc"] < base_scores["auc"] - PRUNE_AUC_TOLERANCE
    ):
        return False

    if (
        candidate_scores["race_hit_rate"]
        < base_scores["race_hit_rate"] - PRUNE_HIT_RATE_TOLERANCE
    ):
        return False

    return True


def _summarize_feature_groups(
    feature_metrics: List[Dict[str, Any]],
    build_profile: Dict[str, Any],
) -> List[Dict[str, Any]]:
    stage_timings = {
        item["stage"]: float(item["elapsed_seconds"])
        for item in build_profile.get("stage_timings", [])
    }
    total_elapsed = float(build_profile.get("total_elapsed_seconds", 0.0))
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for metric in feature_metrics:
        grouped.setdefault(metric["group"], []).append(metric)

    summary: List[Dict[str, Any]] = []
    for group_name, metrics in grouped.items():
        stage_seconds = float(stage_timings.get(group_name, 0.0))
        summary.append(
            {
                "group": group_name,
                "feature_count": len(metrics),
                "stage_elapsed_seconds": round(stage_seconds, 6),
                "stage_share": round(stage_seconds / total_elapsed, 6) if total_elapsed > 0 else 0.0,
                "group_gain_share": round(
                    sum(float(metric["gain_share"]) for metric in metrics),
                    6,
                ),
            }
        )

    return sorted(summary, key=lambda item: item["stage_elapsed_seconds"], reverse=True)


def train_local_model(
    entries_df: pd.DataFrame,
    races_df: pd.DataFrame,
    model_output_path: Path,
    training_start_date: date,
    training_end_date: date,
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    _emit_progress(progress_callback, "train:build_features", status="running")
    frame, build_profile = build_modeling_frame(
        entries_df,
        races_df,
        progress_callback=progress_callback,
        event_prefix="train",
        feature_columns=FEATURE_COLUMNS,
        return_profile=True,
    )
    if frame.empty:
        raise ValueError("学習用の DuckDB データがありません")

    trainable = frame[frame["result_position"].notna()].copy()
    if trainable.empty:
        raise ValueError("result_position を含む学習用データがありません")
    if trainable["is_winner"].sum() <= 0 or trainable["is_winner"].sum() >= len(trainable):
        raise ValueError("学習ラベルが片側に偏りすぎています")

    unique_dates = sorted(trainable["race_date"].dropna().unique().tolist())
    if len(unique_dates) < 2:
        raise ValueError("学習には少なくとも2日分の結果付きデータが必要です")

    split_index = max(1, int(len(unique_dates) * 0.8))
    split_index = min(split_index, len(unique_dates) - 1)
    valid_dates = set(unique_dates[split_index:])
    train_frame = trainable[~trainable["race_date"].isin(valid_dates)].copy()
    valid_frame = trainable[trainable["race_date"].isin(valid_dates)].copy()

    if train_frame.empty or valid_frame.empty:
        raise ValueError("学習・検証データの分割に失敗しました")

    _emit_progress(
        progress_callback,
        "train:split_data",
        training_rows=int(len(train_frame)),
        validation_rows=int(len(valid_frame)),
    )
    base_feature_columns = list(FEATURE_COLUMNS)
    base_model, base_validation_scores = _fit_and_evaluate_model(
        train_frame,
        valid_frame,
        feature_columns=base_feature_columns,
        progress_callback=progress_callback,
        fit_event="train:fit_model",
    )
    base_feature_metrics = _calculate_feature_metrics(
        base_model,
        base_feature_columns,
        build_profile=build_profile,
    )
    feature_plan = _select_feature_plan(base_feature_metrics)
    retained_feature_columns = feature_plan["retained_feature_columns"]
    pruned_features = feature_plan["pruned_features"]
    cost_candidates = _identify_cost_candidates(base_feature_metrics, build_profile)

    final_model = base_model
    final_validation_scores = base_validation_scores
    final_feature_columns = base_feature_columns
    final_feature_metrics = base_feature_metrics
    pruning_applied = False

    if pruned_features and retained_feature_columns != base_feature_columns:
        _emit_progress(
            progress_callback,
            "train:prune_candidates",
            pruned_features=len(pruned_features),
            retained_features=len(retained_feature_columns),
        )
        pruned_model, pruned_validation_scores = _fit_and_evaluate_model(
            train_frame,
            valid_frame,
            feature_columns=retained_feature_columns,
            progress_callback=progress_callback,
            fit_event="train:fit_pruned_model",
        )
        pruned_feature_metrics = _calculate_feature_metrics(
            pruned_model,
            retained_feature_columns,
            build_profile=build_profile,
        )
        if _accept_pruned_model(base_validation_scores, pruned_validation_scores):
            final_model = pruned_model
            final_validation_scores = pruned_validation_scores
            final_feature_columns = retained_feature_columns
            final_feature_metrics = pruned_feature_metrics
            pruning_applied = True

    _emit_progress(progress_callback, "train:evaluate_model", status="running")
    feature_importance = _top_feature_importance(final_feature_metrics)
    feature_audit = {
        "base_feature_count": len(base_feature_columns),
        "retained_feature_count": len(final_feature_columns),
        "pruning_applied": pruning_applied,
        "retired_features": RETIRED_FEATURES,
        "retired_groups": RETIRED_GROUPS,
        "pruned_features": pruned_features if pruning_applied else [],
        "pruned_feature_candidates": pruned_features,
        "cost_candidates": cost_candidates,
        "build_profile": build_profile,
        "group_metrics": _summarize_feature_groups(final_feature_metrics, build_profile),
        "base_validation_scores": base_validation_scores,
        "final_validation_scores": final_validation_scores,
        "feature_metrics": final_feature_metrics,
    }

    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "model_type": "duckdb_lightgbm_win",
        "created_at": datetime.now().isoformat(),
        "training_start_date": training_start_date.isoformat(),
        "training_end_date": training_end_date.isoformat(),
        "feature_columns": final_feature_columns,
        "all_feature_columns": base_feature_columns,
        "validation_scores": final_validation_scores,
        "feature_importance": feature_importance,
        "feature_audit": feature_audit,
        "model": final_model,
    }
    _emit_progress(progress_callback, "train:save_model", model_path=str(model_output_path))
    with model_output_path.open("wb") as handle:
        pickle.dump(payload, handle)

    _emit_progress(
        progress_callback,
        "train:complete",
        model_path=str(model_output_path),
        training_samples=int(len(train_frame)),
        validation_samples=int(len(valid_frame)),
    )
    return {
        "model_path": str(model_output_path),
        "feature_columns": final_feature_columns,
        "validation_scores": final_validation_scores,
        "feature_importance": feature_importance,
        "feature_audit": feature_audit,
        "training_samples": int(len(train_frame)),
        "validation_samples": int(len(valid_frame)),
    }


def load_model_bundle(model_path: str | Path) -> Dict[str, Any]:
    with Path(model_path).open("rb") as handle:
        return pickle.load(handle)


def _normalize_strengths(probabilities: Dict[int, float]) -> Dict[int, float]:
    total = sum(max(value, 1e-6) for value in probabilities.values())
    if total <= 0:
        uniform = 1.0 / max(len(probabilities), 1)
        return {key: uniform for key in probabilities}
    return {key: max(value, 1e-6) / total for key, value in probabilities.items()}


def _compute_ticket_probabilities(
    strengths: Dict[int, float],
) -> Dict[str, Dict[str, float]]:
    normalized = _normalize_strengths(strengths)
    boats = list(normalized.keys())
    total_strength = sum(normalized.values())

    first = normalized.copy()
    second = {boat: 0.0 for boat in boats}
    third = {boat: 0.0 for boat in boats}
    exacta: Dict[str, float] = {}
    quinella: Dict[str, float] = {}
    trifecta: Dict[str, float] = {}
    trio: Dict[str, float] = {}

    for first_boat in boats:
        prob_first = normalized[first_boat] / total_strength
        remaining_after_first = total_strength - normalized[first_boat]
        if remaining_after_first <= 0:
            continue
        for second_boat in boats:
            if second_boat == first_boat:
                continue
            prob_second = prob_first * (normalized[second_boat] / remaining_after_first)
            exacta[f"{first_boat}-{second_boat}"] = prob_second
            quinella_key = "-".join(str(value) for value in sorted([first_boat, second_boat]))
            quinella[quinella_key] = quinella.get(quinella_key, 0.0) + prob_second
            second[second_boat] += prob_second

            remaining_after_second = remaining_after_first - normalized[second_boat]
            if remaining_after_second <= 0:
                continue
            for third_boat in boats:
                if third_boat in {first_boat, second_boat}:
                    continue
                prob_third = prob_second * (normalized[third_boat] / remaining_after_second)
                trifecta[f"{first_boat}-{second_boat}-{third_boat}"] = prob_third
                trio_key = "-".join(
                    str(value) for value in sorted([first_boat, second_boat, third_boat])
                )
                trio[trio_key] = trio.get(trio_key, 0.0) + prob_third
                third[third_boat] += prob_third

    win = {str(boat): probability for boat, probability in first.items()}
    return {
        "win": win,
        "exacta": exacta,
        "quinella": quinella,
        "trifecta": trifecta,
        "trio": trio,
        "_marginals": {
            "first": {boat: first[boat] for boat in boats},
            "second": second,
            "third": third,
        },
    }


def _compute_confidence(first_probabilities: Dict[int, float]) -> float:
    values = np.asarray(list(first_probabilities.values()), dtype=float)
    if values.size == 0:
        return 0.0
    entropy = float(-(values * np.log(np.clip(values, 1e-9, 1.0))).sum())
    max_entropy = math.log(len(values)) if len(values) > 1 else 1.0
    if max_entropy <= 0:
        return 1.0
    return float(max(0.0, min(1.0, 1.0 - (entropy / max_entropy))))


def predict_local_races(
    model_bundle: Dict[str, Any],
    history_entries_df: pd.DataFrame,
    history_races_df: pd.DataFrame,
    target_entries_df: pd.DataFrame,
    target_races_df: pd.DataFrame,
    progress_callback: Optional[ProgressCallback] = None,
) -> List[LocalRacePrediction]:
    if target_entries_df.empty:
        return []

    historical = history_entries_df.copy()
    historical["__target__"] = 0
    target = target_entries_df.copy()
    target["__target__"] = 1

    feature_columns = _resolve_feature_columns(model_bundle.get("feature_columns"))
    combined_entries = pd.concat([historical, target], ignore_index=True, sort=False)
    combined_races = pd.concat([history_races_df, target_races_df], ignore_index=True, sort=False)
    _emit_progress(progress_callback, "predict:build_features", status="running")
    modeling_frame = build_modeling_frame(
        combined_entries,
        combined_races,
        progress_callback=progress_callback,
        event_prefix="predict",
        feature_columns=feature_columns,
    )
    target_frame = modeling_frame[modeling_frame["__target__"] == 1].copy()
    if target_frame.empty:
        return []

    for column in feature_columns:
        if column not in target_frame.columns:
            target_frame[column] = 0.0
    target_frame[feature_columns] = target_frame[feature_columns].fillna(0.0)

    model = model_bundle["model"]
    _emit_progress(progress_callback, "predict:score_entries", total_rows=int(len(target_frame)))
    win_scores = model.predict_proba(target_frame[feature_columns])[:, 1]
    target_frame["win_score"] = win_scores

    predictions: List[LocalRacePrediction] = []
    grouped = list(target_frame.groupby(["race_date", "venue_code", "race_number"], sort=True))
    total_races = len(grouped)
    for index, ((race_date_value, venue_code, race_number), race_frame) in enumerate(grouped, start=1):
        _emit_progress(
            progress_callback,
            "predict:race_scored",
            current=index,
            total=total_races,
            venue_code=f"{int(venue_code):02d}",
            race_number=int(race_number),
        )
        strengths = {
            int(row["boat_number"]): float(row["win_score"])
            for _, row in race_frame.iterrows()
        }
        ticket_data = _compute_ticket_probabilities(strengths)
        marginals = ticket_data.pop("_marginals")
        racer_predictions = {}
        for _, row in race_frame.iterrows():
            boat_number = int(row["boat_number"])
            racer_predictions[boat_number] = {
                "1位": float(marginals["first"].get(boat_number, 0.0)),
                "2位": float(marginals["second"].get(boat_number, 0.0)),
                "3位": float(marginals["third"].get(boat_number, 0.0)),
                "racer_number": int(row["racer_number"]) if row["racer_number"] != -1 else None,
                "racer_name": row.get("racer_name"),
            }

        race_date_python = pd.Timestamp(race_date_value).date()
        predictions.append(
            LocalRacePrediction(
                race_id=int(f"{race_date_python.strftime('%Y%m%d')}{int(venue_code):02d}{int(race_number):02d}"),
                race_date=race_date_python,
                venue_code=f"{int(venue_code):02d}",
                race_number=int(race_number),
                racer_predictions=racer_predictions,
                ticket_probabilities=ticket_data,
                confidence_score=_compute_confidence(marginals["first"]),
                prediction_timestamp=datetime.now(),
            )
        )

    _emit_progress(progress_callback, "predict:complete", total_races=total_races)
    return predictions
