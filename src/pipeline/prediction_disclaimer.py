"""User-facing disclaimer text for BoatRace prediction outputs."""

from __future__ import annotations

from typing import Any, Dict


PREDICTION_DISCLAIMER_TITLE = "利用上の注意"
PREDICTION_DISCLAIMER_TEXT = (
    "この予測と買い目候補は参考情報であり、的中や回収率を保証するものではありません。"
    "実際の収益性はオッズ、購入点数、資金配分、直前情報に大きく左右されます。"
    "そのまま信じて購入してもプラスになるとは限らないため、あくまでレースを楽しむための材料として、"
    "自己責任で利用してください。"
)


def prediction_disclaimer_payload() -> Dict[str, str]:
    return {
        "title": PREDICTION_DISCLAIMER_TITLE,
        "text": PREDICTION_DISCLAIMER_TEXT,
    }


def attach_prediction_disclaimer(payload: Any) -> Any:
    if isinstance(payload, dict):
        payload.setdefault("disclaimer", prediction_disclaimer_payload())
    return payload


def render_prediction_disclaimer_markdown() -> str:
    return f"## {PREDICTION_DISCLAIMER_TITLE}\n\n{PREDICTION_DISCLAIMER_TEXT}\n"
