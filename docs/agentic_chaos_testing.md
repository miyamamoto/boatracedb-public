# Agentic Chaos Testing

この仕組みは、`persona -> scenario -> chaos profile -> transcript grading` の4層で、Codex の会話型テストを効率化するためのものです。

## 目的

- 10人前後の利用者像を固定する
- 各利用者が何を調査したいかをシナリオとして定義する
- 途中で日付修正、スコープ変更、出力形式変更、予算制約、割り込みなどを注入する
- 最終的に最新の要求へ正しく到達したかを transcript から採点する

## 定義ファイル

- 人物像: `tests/agentic/fixtures/personas.yaml`
- シナリオ: `tests/agentic/fixtures/scenarios.yaml`
- カオス注入: `tests/agentic/fixtures/chaos_profiles.yaml`

## 基本コマンド

一覧確認:

```bash
python3 scripts/run_agentic_chaos_tests.py catalog
```

smoke 用のテスト計画:

```bash
python3 scripts/run_agentic_chaos_tests.py plan --matrix smoke
```

実行バンドル生成:

```bash
python3 scripts/run_agentic_chaos_tests.py bundle --matrix smoke --output-dir output/agentic-test-bundles
```

生成されるもの:

- `bundle.json`: ケースの完全定義
- `prompt.md`: テスター/自動実行側が読む進行台本
- `transcript.template.json`: assistant 応答欄だけ空いた記録テンプレート

参照シミュレーションで smoke transcript を自動生成:

```bash
python3 scripts/run_agentic_reference_simulation.py --db-path data/boatrace_pipeline.duckdb --matrix smoke --output-dir output/agentic-test-runs
```

採点:

```bash
python3 scripts/run_agentic_chaos_tests.py grade --transcript-dir output/agentic-test-runs --output-dir output/agentic-test-results
```

## recorder workflow

参照シミュレーションではなく、実際の assistant 応答を順に記録したい場合は recorder を使います。

bundle から transcript を初期化:

```bash
python3 scripts/run_agentic_transcript_recorder.py init \
  --bundle output/agentic-test-bundles/morning-shortlist--relative-date-correction/bundle.json
```

現在の進捗確認:

```bash
python3 scripts/run_agentic_transcript_recorder.py status \
  --transcript output/agentic-test-runs/morning-shortlist--relative-date-correction.transcript.json
```

次の assistant 応答を記録:

```bash
python3 scripts/run_agentic_transcript_recorder.py reply \
  --transcript output/agentic-test-runs/morning-shortlist--relative-date-correction.transcript.json \
  --content-file /tmp/assistant_reply.md
```

特定の step を埋めたい場合:

```bash
python3 scripts/run_agentic_transcript_recorder.py reply \
  --transcript output/agentic-test-runs/morning-shortlist--relative-date-correction.transcript.json \
  --assistant-index 2 \
  --content "了解です。2026-04-24 で固定して見直します。"
```

artifact を付けて最終採点:

```bash
python3 scripts/run_agentic_transcript_recorder.py finalize \
  --transcript output/agentic-test-runs/fukuoka-program-sheet--format-switch-to-pdf.transcript.json \
  --output-dir output/agentic-test-results
```

`finalize` は未記入の assistant turn が残っていると失敗します。途中確認だけしたい場合は `status` を使い、未完の transcript を採点したいときだけ `--allow-incomplete` を付けます。

## transcript 形式

`*.transcript.json` を採点対象にします。

参照シミュレーションは、現在の予測・番組表出力を使って representative transcript を埋めます。Codex 本体の代わりではありませんが、定義・採点・成果物連携が壊れていないかを素早く確認する用途に向いています。

```json
{
  "case_id": "fukuoka-program-sheet--format-switch-to-pdf",
  "scenario_id": "fukuoka-program-sheet",
  "persona_id": "print-kiosk-operator",
  "chaos_profile_id": "format-switch-to-pdf",
  "turns": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "...", "artifacts": ["output/program-sheets/...pdf"]}
  ]
}
```

## matrix の考え方

- `smoke`: 各シナリオに対して代表的な chaos を1つだけ付ける
- `recovery`: 各シナリオに対して recovery 系 chaos を2つまで付ける
- `full`: 互換 chaos を全部付ける

## 評価観点

- 最終的にユーザーの最新版要求へ到達したか
- 本命・相手・PDF・予算など、成果物として必要な要素が揃ったか
- 割り込みや要求変更のあとで元のゴールへ復帰できたか
- ユーザーが不要とする backend 詳細を出しすぎていないか

## 運用のコツ

- 日次の回帰には `smoke`
- prompt/skill 変更の確認には `recovery`
- 大きな改修前後の比較には `full`
- transcript はケースごとに保存し、`grade-report.json` を差分比較すると崩れが見やすいです
