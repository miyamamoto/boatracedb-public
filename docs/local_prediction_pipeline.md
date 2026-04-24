# Local Prediction Pipeline

このドキュメントは、DuckDB ベースのローカル予測パイプラインを対象にしています。現在の導線は `fetch -> train -> predict -> query` です。

## 対象範囲

- ローカル cache またはリモート取得データの DuckDB 取り込み
- ローカルモデルの学習
- 対象日の予測
- 予測結果の CLI 参照

この導線はローカル予測と skill 連携だけを対象にします。

## 前提

- Python 3.11 以上
- macOS / Linux は標準対応
- Windows は core bootstrap path を PowerShell 経由で利用可能
- 既定の DuckDB パス: `data/boatrace_pipeline.duckdb`

## インストール

最短導線はリモート installer です。

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash
```

対象日や学習期間を指定する場合:

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash -s -- --target-date 2026-04-24 --training-days 90
```

手動で clone する場合:

```bash
git clone https://github.com/miyamamoto/boatracedb-public.git
cd boatracedb

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e .
```

これで以下の CLI が使えます。

- `boatrace-local-pipeline`
- `boatrace-prediction-query`
- `boatrace-bootstrap`
- `boatrace-program-sheet`

依存導入から fetch/train/predict/skill install まで 1 回で済ませる場合は、次が最短です。

```bash
bash scripts/install_boatrace_local.sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_boatrace_local.ps1
```

内部では `scripts/boatrace_bootstrap.py` を呼び、既定で直近 90 日を学習窓として当日予測まで進めます。train は毎回ではなく、既定で 7 日間隔です。対象日や窓長を変えたい場合は、そのまま引数を渡せます。

```bash
bash scripts/install_boatrace_local.sh --target-date 2026-04-23 --training-days 120
```

再学習間隔を明示したい場合:

```bash
bash scripts/install_boatrace_local.sh --target-date 2026-04-24 --training-days 90 --retrain-interval-days 7
```

インストールしない場合の直接実行パス:

- `python3 scripts/boatrace_local_pipeline.py`
- `python3 scripts/boatrace_prediction_query.py`

補足:

- `scripts/install_boatrace_local.sh` は POSIX シェル前提です
- `scripts/install_boatrace_local.ps1` は Windows 向けの core bootstrap path です
- 会話型 recorder など `fcntl` に依存する補助ツールは Windows 非対応です

## 保存先

- DuckDB レジストリ: `data/boatrace_pipeline.duckdb`
- 学習済みモデル: `models/duckdb_local_model_*.pkl`
- 予測スナップショット: `output/predictions/YYYY-MM-DD/`

DuckDB には主に次のテーブルが入ります。

- `fetch_runs`
- `models`
- `prediction_runs`
- `race_predictions`
- `ticket_predictions`
- `races_prerace`
- `race_entries_prerace`
- `race_results`
- `odds_data`

`races_prerace` / `race_entries_prerace` は schedule 由来の予測入力、`race_results` は performance 由来の結果データです。学習は両者を結合して行い、予測は pre-race テーブルだけを読みます。

現在のモデル特徴量は、艇番・年齢・体重・級別・モーター/ボート率などの pre-race 列と、過去結果から積み上げる履歴集計が中心です。`exhibition_time`、`st_timing`、target date の `result_position` は予測特徴量から外しています。

## データ取得の範囲と頻度

- `fetch` で指定できる期間は `2005-01-01` から当日まで
- 取得頻度は DuckDB ローカル版では手動
- `fetch` は `data/comprehensive_cache` を優先して読み込む
- 不足日だけリモート取得したい場合は `--download-missing` を使う

旧実装で作った DuckDB には pre-race safe テーブルがない場合があります。その場合は `fetch` を再実行して、schedule/results 分離済みのテーブルを作り直してください。

日次更新を自動化したい場合は、外部 scheduler から CLI を呼び出してください。

### cron 例

サンプルファイル:

- `config/boatrace-bootstrap.cron.example`

例:

```cron
10 5 * * * cd /absolute/path/to/boatracedb && /absolute/path/to/boatracedb/.venv/bin/python /absolute/path/to/boatracedb/scripts/boatrace_bootstrap.py --training-days 90 --retrain-interval-days 7 >> /absolute/path/to/boatracedb/logs/cron/boatrace-bootstrap.log 2>&1
```

この構成では `fetch / predict` は毎日実行され、`train` は bootstrap 側の判定で既定 7 日間隔に抑制されます。

### Windows Task Scheduler 例

Windows では cron の代わりに Task Scheduler を使います。

```powershell
schtasks /Create /SC DAILY /ST 05:10 /TN "BoatRaceBootstrap" /TR "powershell -ExecutionPolicy Bypass -File C:\path\to\boatracedb\scripts\install_boatrace_local.ps1 --training-days 90 --retrain-interval-days 7"
```

## コマンド

### 状態確認

```bash
boatrace-local-pipeline status
```

返却内容には、DuckDB パス、fetch 回数、モデル数、予測実行回数、source テーブル件数、active model などが含まれます。

### データ取得

```bash
boatrace-local-pipeline fetch \
  --start-date 2022-03-20 \
  --end-date 2022-03-27
```

オプション:

- `--cache-dir`: cache の読み込み元を変更する
- `--download-missing`: cache にない日付だけリモート取得する
- `--dry-run`: 取得対象だけ確認して保存しない

例:

```bash
boatrace-local-pipeline fetch \
  --start-date 2022-03-20 \
  --end-date 2022-03-27 \
  --cache-dir data/comprehensive_cache \
  --download-missing
```

### 学習

```bash
boatrace-local-pipeline train \
  --training-start-date 2022-03-20 \
  --training-end-date 2022-03-26
```

オプション:

- `--model-type`: 既定値は `lightgbm`
- `--no-activate`: 学習後に active model を切り替えない

出力:

- `models/duckdb_local_model_*.pkl`
- DuckDB の `models` テーブル更新

### 予測

```bash
boatrace-local-pipeline predict \
  --target-date 2022-03-27
```

オプション:

- `--model-path`: active model ではなく明示したモデルを使う
- `--limit`: 対象レース数を制限する

出力:

- `prediction_runs`
- `race_predictions`
- `ticket_predictions`
- `output/predictions/YYYY-MM-DD/latest.json`
- `output/predictions/YYYY-MM-DD/latest.md`

### スナップショット再出力

```bash
boatrace-local-pipeline export \
  --prediction-run-id 05d1d628f2474ccc9966b0cbdbe54f91
```

既存の予測 run を再度 JSON と Markdown に出力します。

## 参照 CLI

### 最新 run

```bash
boatrace-prediction-query --format markdown latest
```

### 日付別

```bash
boatrace-prediction-query --format markdown date \
  --target-date 2022-03-27
```

### レース別

```bash
boatrace-prediction-query --format markdown race \
  --target-date 2022-03-27 \
  --venue-code 03 \
  --race-number 1
```

### active model

```bash
boatrace-prediction-query --format json model
```

### パイプライン状態

```bash
boatrace-prediction-query --format markdown status
```

## skill 連携

- Codex 向け skill: `skills/boatrace-predictions/SKILL.md`
- Claude Code 向け skill: `skills/boatrace-predictions/SKILL.md`
- Claude 向け agent: `.claude/agents/boatrace-predictions.md`

bootstrap は Codex には `~/.codex/skills`、Claude Code には `~/.claude/skills`、Claude agent には `~/.claude/agents` へ配置します。いずれも DuckDB から最新予測、モデル状態、日付別・レース別予測を参照する前提です。

## README との整合

README はこのドキュメントの短縮版です。導入、主要コマンド、保存先、取得範囲、運用前提はここに合わせています。
