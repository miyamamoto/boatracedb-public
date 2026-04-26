# BoatRace Local Predictor

ボートレースの出走データをローカルに保存し、手元の端末で予測を作るためのツールです。予測結果は Codex / Claude / Claude Code から自然文で確認でき、必要に応じて Markdown や PDF のレポートも作れます。

このツールでできること:

- 本日・明日のレース予測を見る
- 注目レース、軸候補、波乱候補をまとめた全体レポートを作る
- 新聞風の PDF や印刷用の番組表 PDF を作る
- 選手の過去実績、当地実績、モーター実績を踏まえてレースを深掘りする
- DuckDB に保存したデータを読み取り専用 SQL で安全に分析する
- Codex / Claude / Claude Code の skill や MCP からローカル予測を呼び出す

予測や買い目候補は参考情報です。的中や回収率は保証しません。舟券購入やその他の判断は、オッズ、購入点数、資金配分、直前情報も含めて自己責任で行ってください。

## すぐ使う

### macOS / Linux

次の 1 コマンドで、アプリ取得、専用 Python 環境作成、依存関係導入、データ取得、学習、予測、skill/MCP 導入まで実行します。

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash
```

git、Python、LightGBM を事前に入れておく必要はありません。installer が `uv` を使って、アプリ専用の Python 3.11 環境を作ります。

### Windows

PowerShell で実行してください。

```powershell
git clone https://github.com/miyamamoto/boatracedb-public.git $HOME\boatracedb
cd $HOME\boatracedb
powershell -ExecutionPolicy Bypass -File .\scripts\install_boatrace_local.ps1
```

Windows でも主な bootstrap 処理は動きます。定期実行は cron ではなく、Windows タスク スケジューラを使ってください。

## 初回セットアップで行うこと

初回は次の処理を順番に行います。時間がかかるため、installer はスピナー、現在の処理、経過時間、残り時間の目安を表示します。

1. アプリ一式を `~/boatracedb` に配置
2. アプリ専用 Python 3.11 環境を作成
3. DuckDB、LightGBM などの依存関係を導入
4. 過去データを取得
5. 特徴量を作成
6. ローカルモデルを学習
7. 本日または指定日の予測を生成
8. Codex / Claude / Claude Code skill と MCP を導入

所要時間の目安:

- 180日分: 標準。データ取得だけで約1時間、全体で約1.5から2.5時間
- 365日分: 年間分析向け。全体で約3から5時間
- 730日分: 中長期分析向け。全体で約6から10時間
- 1095日分: 長期研究向け。全体で約9から15時間

端末性能、ネットワーク、取得済み cache の量で大きく変わります。特徴量作成とモデル学習は特に時間がかかります。

## データ取得と cache

データ取得は cache 優先です。`data/comprehensive_cache` にある日付はリモート取得せず、足りない日付だけ取得します。

既存インストールを更新する場合、既定では不足データをリモート取得しません。アプリ本体、skill、MCP を更新し、既存の DuckDB、cache、モデル、予測結果は保持します。

不足データも取得したい場合:

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash -s -- --download-missing
```

手元の cache だけで進めたい場合:

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash -s -- --cache-only
```

SQL 分析用に投入する過去実績日数は、初回セットアップ時に選べます。既定は 180 日です。

```bash
bash scripts/install_boatrace_local.sh --training-days 90 --analysis-days 365
```

## 普段の使い方

本日の予測を見る:

```bash
boatrace-prediction-query --format markdown today
```

明日の予測を見る:

```bash
boatrace-prediction-query --format markdown tomorrow
```

最新の予測を見る:

```bash
boatrace-prediction-query --format markdown latest
```

本日/明日分がまだ無い場合は、既存モデルを使って自動で `fetch -> predict` を行ってから表示します。ここでは再学習は行いません。再学習は週1回程度の bootstrap 側に任せます。

## レポートと PDF

全体予測レポートを Markdown / PDF で作る:

```bash
boatrace-prediction-report --latest --format markdown
```

出力先:

```text
output/prediction-reports/YYYY-MM-DD/prediction-report.md
output/prediction-reports/YYYY-MM-DD/prediction-report.pdf
```

レポートには次を含めます。

- 注目レース
- 軸候補が強いレース
- 波乱・相手探し候補
- 外枠・穴の拾いどころ
- 会場別サマリー
- confidence 帯別の件数
- 本命艇番分布
- 券種別の上位候補平均

印刷用の番組表 PDF を作る:

```bash
boatrace-program-sheet --target-date 2026-04-24 --venue-code 22
```

出力先:

```text
output/program-sheets/YYYY-MM-DD/
```

## レースを深掘りする

Claude / Codex から「平和島9Rを選手の過去実績も踏まえて教えて」のように聞くと、予測、出走選手、選手過去実績、当地実績、モーター実績をまとめて確認します。

Claude / Codex の回答では、内部の SQL や DB の説明ではなく、次のように何を確認しているかを表示する設計です。

```text
まず平和島9Rの予測と上位候補を確認します。
次に、このレースの出走選手の過去成績を確認します。
当地実績とモーター成績を照らし合わせます。
最後に、予測値と過去実績を合わせて見立てを整理します。
```

## SQL 分析

安全な読み取り専用ビューを確認する:

```bash
boatrace-analysis-query schema
```

選手成績を分析する:

```bash
boatrace-analysis-query query --sql "SELECT racer_name, starts, win_rate, top3_rate FROM analysis_racer_summary ORDER BY win_rate DESC LIMIT 20"
```

SQL 分析は `analysis_*` ビューだけを許可します。DDL/DML、ファイル読込、複数ステートメント、外部 DB attach などは拒否します。

## Skill / MCP 連携

installer は次を自動配置します。

- Codex: `~/.codex/skills/boatrace-predictions`
- Codex: `~/.codex/skills/boatrace-program-sheet`
- Claude Code: `~/.claude/skills/boatrace-predictions`
- Claude Code: `~/.claude/skills/boatrace-program-sheet`
- Claude: `~/.claude/agents/boatrace-predictions.md`
- Claude: `~/.claude/agents/boatrace-program-sheet.md`

Claude Code / Claude Desktop には `boatrace-local` MCP server も登録します。ローカル DuckDB を読み取り専用で参照します。

主な MCP tool:

- `boatrace_status`: DB、モデル、予測の状態確認
- `boatrace_latest_predictions`: 最新予測一覧
- `boatrace_race_prediction`: 指定レースの予測
- `boatrace_race_deep_analysis`: 指定レースの予測、選手実績、当地実績、モーター実績
- `boatrace_analysis_schema`: SQL 分析用ビューの確認
- `boatrace_safe_analysis_query`: 読み取り専用 SQL 分析

## 更新

macOS / Linux は同じ install command を再実行します。

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash
```

Windows:

```powershell
cd $HOME\boatracedb
git pull
powershell -ExecutionPolicy Bypass -File .\scripts\install_boatrace_local.ps1
```

更新時は既存の `data/`, `models/`, `.venv` を保持します。

## アンインストール

macOS / Linux:

```bash
rm -rf ~/boatracedb
rm -rf ~/.codex/skills/boatrace-predictions ~/.codex/skills/boatrace-program-sheet
rm -rf ~/.claude/skills/boatrace-predictions ~/.claude/skills/boatrace-program-sheet
rm -f ~/.claude/agents/boatrace-predictions.md ~/.claude/agents/boatrace-program-sheet.md
```

Windows は `$HOME\boatracedb` を削除し、Claude / Codex 側に配置した skill や MCP 設定を削除してください。

## 保存先

- DuckDB: `data/boatrace_pipeline.duckdb`
- 取得 cache: `data/comprehensive_cache`
- モデル: `models/duckdb_local_model_*.pkl`
- 予測出力: `output/predictions/YYYY-MM-DD/`
- 全体予測レポート: `output/prediction-reports/YYYY-MM-DD/`
- 番組表 PDF: `output/program-sheets/YYYY-MM-DD/`

## 必要環境

通常の macOS / Linux installer では、git や Python の事前準備は不要です。Windows は git clone を使うため Git が必要です。

macOS で LightGBM 読み込みに失敗する場合:

```bash
brew install libomp
```

## License

Apache License 2.0 です。

商用利用の場合は事前にご連絡ください。

詳細な運用手順は `docs/local_prediction_pipeline.md` を参照してください。
