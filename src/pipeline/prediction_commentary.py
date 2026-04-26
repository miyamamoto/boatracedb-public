"""Lightweight user-facing commentary for prediction snapshots."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


TICKET_TYPE_LABELS = {
    "win": "単勝",
    "exacta": "2連単",
    "quinella": "2連複",
    "trifecta": "3連単",
    "trio": "3連複",
}


def _probability(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _boat_label(item: Dict[str, Any]) -> str:
    return f"{item.get('racer_id', '?')}号艇"


def _confidence_label(confidence: float) -> str:
    if confidence >= 0.60:
        return "鉄板寄り"
    if confidence >= 0.45:
        return "本命寄り"
    if confidence >= 0.30:
        return "軸はいるが相手探し"
    return "波乱含み"


def _top3(race: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(race.get("top3") or [])[:3]


def _win_probability(item: Dict[str, Any]) -> float:
    return _probability(item.get("win_probability"))


def _top_gap(top3: List[Dict[str, Any]]) -> float:
    if len(top3) < 2:
        return _win_probability(top3[0]) if top3 else 0.0
    return _win_probability(top3[0]) - _win_probability(top3[1])


def _race_name(race: Dict[str, Any]) -> str:
    venue = race.get("venue_name") or race.get("venue_code") or "unknown"
    return f"{venue} {race.get('race_number', '?')}R"


def _ticket_summary(race: Dict[str, Any]) -> Optional[str]:
    ticket_predictions = race.get("ticket_predictions") or {}
    pieces: List[str] = []
    for ticket_type in ("trifecta", "exacta", "trio", "quinella", "win"):
        combinations = ticket_predictions.get(ticket_type) or []
        if not combinations:
            continue
        top = combinations[0]
        probability = _probability(top.get("probability"))
        ticket_label = TICKET_TYPE_LABELS.get(ticket_type, ticket_type)
        pieces.append(f"{ticket_label} {top.get('combination')}({probability:.1%})")
        if len(pieces) >= 2:
            break
    if not pieces:
        return None
    return " / ".join(pieces)


def _race_shape_sentence(race: Dict[str, Any]) -> str:
    top3 = _top3(race)
    if not top3:
        return "まだ上位候補を読み切れる材料が少ないレースです。"

    top = top3[0]
    top_probability = _win_probability(top)
    gap = _top_gap(top3)
    confidence = _probability(race.get("confidence_score"))
    label = _confidence_label(confidence)

    if gap >= 0.20:
        shape = f"{_boat_label(top)}がかなり抜けた評価で、軸を決めやすい形です"
    elif gap >= 0.10:
        shape = f"{_boat_label(top)}中心ですが、2番手との差は絶対的ではありません"
    elif len(top3) >= 2:
        shape = f"{_boat_label(top)}と{_boat_label(top3[1])}の差が小さく、頭争いは接戦です"
    else:
        shape = f"{_boat_label(top)}を上位に見ています"

    return f"{shape}。モデル上の1着確率は{top_probability:.1%}、レース全体の見立ては「{label}」です。"


def _opponent_sentence(race: Dict[str, Any]) -> str:
    top3 = _top3(race)
    if len(top3) < 2:
        return "相手候補はまだ絞り込みすぎず、直前気配も合わせて確認したいところです。"
    opponents = "、".join(f"{_boat_label(item)}({_win_probability(item):.1%})" for item in top3[1:])
    gap = _top_gap(top3)
    if gap < 0.10:
        return f"相手というより逆転候補として {opponents} を強めに見たいレースです。"
    return f"相手候補は {opponents}。本命から入る場合も、2着・3着の入れ替わりに注意です。"


def _buying_sentence(race: Dict[str, Any]) -> str:
    ticket = _ticket_summary(race)
    top3 = _top3(race)
    gap = _top_gap(top3)
    if ticket:
        base = f"買い目候補では {ticket} が上位です。"
    else:
        base = "買い目は確率上位から組むなら、軸を決めて相手を広げる形が基本です。"
    if gap >= 0.20:
        return base + "本線は頭固定、妙味を見るなら2着・3着側のズレを拾う組み方です。"
    if gap < 0.10:
        return base + "ここは頭固定に寄せすぎず、上位同士の折り返しや押さえを考えたい形です。"
    return base + "本線と押さえを分け、人気が被る組み合わせはオッズを見て点数を絞るのが現実的です。"


def _analysis_note(race: Dict[str, Any]) -> str:
    top3 = _top3(race)
    gap = _top_gap(top3)
    ticket_predictions = race.get("ticket_predictions") or {}
    trifecta_count = len(ticket_predictions.get("trifecta") or [])
    if len(top3) >= 3 and gap < 0.08:
        return "分析メモ: 上位の確率差が小さいため、的中率だけを追うと点数が増えやすいレースです。オッズが付かない組み合わせは無理に厚くしない方が扱いやすいです。"
    if top3 and _win_probability(top3[0]) >= 0.45 and trifecta_count >= 3:
        return "分析メモ: 軸候補ははっきりしていますが、3連単は相手順のズレで外れやすい券種です。軸の強さと相手の広がりを分けて考えると整理しやすくなります。"
    if len(top3) >= 3:
        return "分析メモ: 上位3艇で形は作れますが、回収率は確率よりオッズとの釣り合いが重要です。低配当なら見送りや点数削減も選択肢です。"
    return "分析メモ: 予測値は事前情報ベースです。展示気配や直前オッズで評価が変わる余地があります。"


def render_race_commentary_markdown(race: Dict[str, Any]) -> str:
    """Render a compact but deeper Japanese explanation for one race."""
    lines = [
        "## 見立て",
        "",
        f"- {_race_shape_sentence(race)}",
        f"- {_opponent_sentence(race)}",
        f"- {_buying_sentence(race)}",
        "",
        _analysis_note(race),
    ]
    return "\n".join(lines)


def _sort_by_confidence(races: Iterable[Dict[str, Any]], reverse: bool = True) -> List[Dict[str, Any]]:
    return sorted(races, key=lambda race: _probability(race.get("confidence_score")), reverse=reverse)


def render_run_commentary_markdown(run: Dict[str, Any]) -> str:
    """Render a daily overview that highlights races worth reading first."""
    races = list(run.get("races") or [])
    if not races:
        return "## 今日の見立て\n\n予測対象レースが見つかりませんでした。"

    strongest = _sort_by_confidence(races)[:3]
    volatile = [
        race for race in races
        if len(_top3(race)) >= 2 and _top_gap(_top3(race)) < 0.08
    ][:3]
    outside = [
        race for race in races
        if any(int(item.get("racer_id", 0) or 0) >= 4 for item in _top3(race)[:3])
    ][:3]

    lines = ["## 今日の見立て", ""]
    lines.append(
        f"全体では {len(races)} レースを確認しました。まずは confidence が高いレースを本線候補、上位差が小さいレースを波乱候補として分けて見るのが扱いやすいです。"
    )

    lines.extend(["", "### 本線候補", ""])
    for race in strongest:
        top3 = _top3(race)
        top = top3[0] if top3 else {}
        lines.append(
            f"- {_race_name(race)}: {_boat_label(top)}中心、confidence={_probability(race.get('confidence_score')):.3f}。{_race_shape_sentence(race)}"
        )

    if volatile:
        lines.extend(["", "### 波乱・相手探し候補", ""])
        for race in volatile:
            top3 = _top3(race)
            names = " vs ".join(_boat_label(item) for item in top3[:2])
            lines.append(f"- {_race_name(race)}: {names} の差が小さく、頭固定より相手関係の読みが重要です。")

    if outside:
        lines.extend(["", "### 外枠・穴の拾いどころ", ""])
        for race in outside:
            top3 = _top3(race)
            outsiders = [item for item in top3 if int(item.get("racer_id", 0) or 0) >= 4]
            outsider_text = "、".join(f"{_boat_label(item)}({_win_probability(item):.1%})" for item in outsiders)
            lines.append(f"- {_race_name(race)}: {outsider_text} が上位候補に入っており、ヒモ荒れを少し意識したいレースです。")

    lines.extend(
        [
            "",
            "分析メモ: 本命レースほど配当は薄くなりやすく、混戦レースほど点数が増えやすくなります。予測確率だけでなく、オッズと点数のバランスで見送りも含めて判断するのが現実的です。",
        ]
    )
    return "\n".join(lines)


def attach_prediction_commentary(payload: Any) -> Any:
    """Attach generated commentary to run or race payloads without mutating non-dicts."""
    if not isinstance(payload, dict):
        return payload
    if payload.get("races") is not None:
        payload.setdefault("commentary_markdown", render_run_commentary_markdown(payload))
        for race in payload.get("races") or []:
            if isinstance(race, dict):
                race.setdefault("commentary_markdown", render_race_commentary_markdown(race))
        return payload
    if payload.get("top3") is not None:
        payload.setdefault("commentary_markdown", render_race_commentary_markdown(payload))
    return payload
