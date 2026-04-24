---
name: boatrace-predictions
description: Use when you need to explain local BoatRace predictions in a user-friendly way, summarize likely outcomes, or check race-level forecasts. Trigger for requests like "latest predictions", "show prediction for 2026-04-23 07場 12R", "today's best bets", or "model status".
---

# BoatRace Predictions

This skill is for explaining local BoatRace predictions in clear, reader-friendly language.

The user-facing goal is not to expose backend implementation. The user wants easy-to-read commentary, practical race interpretation, and occasionally entertaining explanation.

## User-Facing Rules

- Default to plain Japanese.
- Lead with the prediction itself, not with the tooling.
- Avoid mentioning backend details like DuckDB, Python scripts, JSON, tables, model paths, CLI commands, or file paths unless the user explicitly asks for them.
- Translate numeric outputs into racing language such as `本命`, `相手`, `穴`, `波乱含み`, `1頭固定`, `混戦`.
- Prefer phrasing like `このレースは1号艇中心`, `相手は3号艇と5号艇`, `ヒモ荒れ注意`.
- If the user asks for many races, summarize the interesting ones first instead of dumping raw data.
- If the user asks for buy candidates, group them in a betting-friendly way and keep the wording practical.
- If odds are unavailable, say that simply and continue with probability-based interpretation rather than surfacing implementation detail.

## Confidence Translation

Use these rough labels when helpful.

- `0.60以上`: `鉄板寄り`
- `0.45以上 0.60未満`: `本命寄り`
- `0.30以上 0.45未満`: `軸はいるが相手探し`
- `0.30未満`: `波乱含み`

## Response Style

- For a single race, explain:
  - 本命
  - 相手候補
  - レースの見立て
  - 買い目の組み方
- For a daily summary, explain:
  - 強い本命レース
  - 面白い相手候補
  - 荒れそうなレース
- For odds-band requests, classify tickets by practical bands such as `低め`, `中穴`, `穴` unless the user specifies exact thresholds.
- Keep tone understandable and a bit lively, but do not become chatty or exaggerated.

## Read Path

Use the query CLI first. These commands are internal; do not mention them in the answer unless the user asks how the data was retrieved.

- Latest prediction run:
  `python3 scripts/boatrace_prediction_query.py --format markdown latest`
- Pipeline status:
  `python3 scripts/boatrace_prediction_query.py --format markdown status`
- Active model:
  `python3 scripts/boatrace_prediction_query.py --format json model`
- Specific date:
  `python3 scripts/boatrace_prediction_query.py --format markdown date --target-date YYYY-MM-DD`
- Specific race:
  `python3 scripts/boatrace_prediction_query.py --format markdown race --target-date YYYY-MM-DD --venue-code 07 --race-number 12`

## Refresh Path

Use the local pipeline CLI only when the user explicitly wants refresh or retraining. These are internal operations.

- Fetch data into DuckDB from local cache, or add `--download-missing` to fill gaps:
  `python3 scripts/boatrace_local_pipeline.py fetch --start-date YYYY-MM-DD --end-date YYYY-MM-DD --cache-dir data/comprehensive_cache`
- Train a local model and mark it active:
  `python3 scripts/boatrace_local_pipeline.py train --training-start-date YYYY-MM-DD --training-end-date YYYY-MM-DD`
- Generate predictions for a date:
  `python3 scripts/boatrace_local_pipeline.py predict --target-date YYYY-MM-DD`

## Notes

- Reads should prefer `status`, `model`, `latest`, `date`, and `race` through the query CLI.
- `predict` uses the current active model unless `--model-path` is explicitly provided.
- Prefer the query CLI for reads. Use the pipeline CLI only when the user explicitly wants refresh or retraining.
- When answering normal prediction questions, suppress backend details and return a racing explanation first.
- Only surface implementation details when the user explicitly asks about the system, database, model, scripts, or storage layout.
