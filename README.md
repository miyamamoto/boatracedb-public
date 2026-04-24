# BoatRace Local Predictor

ローカル端末だけでボートレース予測を作り、その結果を Codex / Claude の skill から読めるようにするための最小構成です。

目的は次の 2 点です。

- リモートからデータを取得し、DuckDB とローカルモデルで予測する
- 予測結果を `boatrace-predictions` / `boatrace-program-sheet` skill から使う

## 1 コマンド導入

macOS / Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash
```

対象日や学習期間を指定する場合:

```bash
curl -fsSL https://raw.githubusercontent.com/miyamamoto/boatracedb-public/main/scripts/install_remote.sh | bash -s -- --target-date 2026-04-24 --training-days 90
```

Windows PowerShell:

```powershell
git clone https://github.com/miyamamoto/boatracedb-public.git $HOME\boatracedb
cd $HOME\boatracedb
powershell -ExecutionPolicy Bypass -File .\scripts\install_boatrace_local.ps1
```

初回導入では、リポジトリ取得、仮想環境作成、依存関係導入、データ取得、特徴量作成、モデル学習、予測、skill/agent 配置まで実行します。既定の学習期間は直近 90 日、再学習間隔は 7 日です。

## 使い方

最新予測を見る:

```bash
boatrace-prediction-query --format markdown latest
```

モデルとデータの状態を見る:

```bash
boatrace-local-pipeline status
```

指定日の予測を作る:

```bash
boatrace-local-pipeline predict --target-date 2026-04-24
```

印刷用の番組表 PDF を作る:

```bash
boatrace-program-sheet --target-date 2026-04-24 --venue-code 22
```

## Skill 連携

導入後、次のファイルが自動配置されます。

- Codex: `~/.codex/skills/boatrace-predictions`
- Codex: `~/.codex/skills/boatrace-program-sheet`
- Claude Code: `~/.claude/skills/boatrace-predictions`
- Claude Code: `~/.claude/skills/boatrace-program-sheet`
- Claude: `~/.claude/agents/boatrace-predictions.md`
- Claude: `~/.claude/agents/boatrace-program-sheet.md`

反映には Codex / Claude の再起動が必要です。通常の質問では backend の詳細を出さず、予測の見立て、買い目候補、番組表の要点を自然な説明として返す設計です。

## 保存先

- DuckDB: `data/boatrace_pipeline.duckdb`
- 取得 cache: `data/comprehensive_cache`
- モデル: `models/duckdb_local_model_*.pkl`
- 予測出力: `output/predictions/YYYY-MM-DD/`
- 番組表 PDF: `output/program-sheets/YYYY-MM-DD/`

## 必要環境

- Python 3.11+
- Git
- ネットワーク接続
- macOS で LightGBM 読み込みに失敗する場合は `brew install libomp`

## License

Apache License 2.0 です。

商用利用の場合は事前にご連絡ください。

詳細な運用手順は `docs/local_prediction_pipeline.md` を参照してください。
