"""User-facing disclaimer text for BoatRace prediction outputs."""

from __future__ import annotations

from typing import Any, Dict


PREDICTION_DISCLAIMER_TITLE = "利用上の注意"
PREDICTION_DISCLAIMER_SHORT_TEXT = (
    "予測と買い目候補は参考情報です。購入判断はオッズや直前情報も含めて自己責任でお願いします。"
)
PREDICTION_DISCLAIMER_TEXT = (
    "この予測と買い目候補は参考情報であり、的中や回収率を保証するものではありません。"
    "実際の収益性はオッズ、購入点数、資金配分、直前情報に大きく左右されます。"
    "そのまま信じて購入してもプラスになるとは限らないため、あくまでレースを楽しむための材料として、"
    "自己責任で利用してください。"
)


def prediction_disclaimer_payload(*, include_detail: bool = False) -> Dict[str, str]:
    payload = {
        "title": PREDICTION_DISCLAIMER_TITLE,
        "short_text": PREDICTION_DISCLAIMER_SHORT_TEXT,
    }
    if include_detail:
        payload["text"] = PREDICTION_DISCLAIMER_TEXT
    return payload


def attach_prediction_disclaimer(payload: Any) -> Any:
    if isinstance(payload, dict):
        payload.setdefault("disclaimer", prediction_disclaimer_payload())
    return payload


def render_prediction_disclaimer_markdown(*, compact: bool = True) -> str:
    if compact:
        return f"> 注: {PREDICTION_DISCLAIMER_SHORT_TEXT}\n"
    return f"## {PREDICTION_DISCLAIMER_TITLE}\n\n{PREDICTION_DISCLAIMER_TEXT}\n"
