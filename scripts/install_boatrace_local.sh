#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
UV_INSTALL_DIR="${ROOT_DIR}/.tools/bin"
UV_BIN="${UV_BIN:-}"
LOG_DIR="${ROOT_DIR}/logs/bootstrap-install"

mkdir -p "${LOG_DIR}"

format_elapsed() {
  local seconds="$1"
  printf "%02d:%02d" "$((seconds / 60))" "$((seconds % 60))"
}

print_uv_install_help() {
  cat >&2 <<'EOF'

Python 3.11 と LightGBM などの依存関係は、installer がアプリ専用環境へ自動導入します。
そのため通常は Python を手で入れる必要はありません。

ただし、Python runtime manager の uv を入れるために curl が必要です。
curl が無い場合は次を入れてから再実行してください。

Ubuntu / Debian:
  sudo apt update
  sudo apt install -y curl

Fedora:
  sudo dnf install -y curl

macOS:
  curl は通常入っています。無い場合は Command Line Tools か Homebrew を確認してください。
EOF
}

print_header() {
  cat <<'EOF'
BoatRace Local Predictor installer

これから次の順でセットアップします。
  1. Python/依存管理ツール uv を確認または導入
  2. アプリ専用の Python 3.11 環境を作成
  3. DuckDB、LightGBM などの依存関係を導入
  4. 過去データを取得
  5. 特徴量を作成してローカルモデルを学習
  6. 対象日の予測を生成
  7. Codex / Claude Code skill と Claude MCP を導入

注意:
  - Python 3.11 や LightGBM はアプリ専用環境に自動導入します。
  - 初回はデータ取得、特徴量作成、LightGBM 学習に時間がかかります。
  - SQL分析用に投入する過去データ量もこのあと確認します。
  - Claude Code / Claude Desktop には読み取り専用の boatrace-local MCP server を登録します。
  - このシステムの予測、買い目候補、SQL分析結果、番組表、説明文は参考情報です。
  - 開発者および配布者は出力の正確性や利用結果について一切の責任を負いません。
  - 舟券購入やその他の判断は、必ず利用者自身の責任で行ってください。
  - 180日分ではデータ取得だけで約1時間かかる前提です。
  - 180日分の初回セットアップ全体は、おおよそ1.5から2.5時間を見込んでください。
  - 詳細ログは logs/bootstrap-install/ に保存します。
EOF
  printf '\n'
}

has_arg() {
  local name="$1"
  shift
  for arg in "$@"; do
    if [[ "${arg}" == "${name}" || "${arg}" == "${name}="* ]]; then
      return 0
    fi
  done
  return 1
}

select_analysis_days() {
  local answer
  local custom
  cat >/dev/tty <<'EOF'
SQL分析用にDuckDBへ投入する過去実績日数を選んでください。
目的により必要な履歴量が変わります。180日が最小で、標準です。
目安として、180日分ではデータ取得だけで約1時間、初回全体で1.5から2.5時間程度かかります。

  1) 180日  標準: 直近傾向、今日/明日の予測説明、軽い選手確認。初回合計 約1.5-2.5時間
  2) 365日  年間分析: 選手・モーターの年間傾向、会場別の比較。初回合計 約3-5時間
  3) 730日  中長期分析: 季節差、会場相性、選手の変化を広めに確認。初回合計 約6-10時間
  4) 1095日 長期分析: 3年程度の傾向、長期比較、研究用途。初回合計 約9-15時間
  5) カスタム 180日以上で指定

選択 [1]:
EOF
  IFS= read -r answer </dev/tty || true
  answer="${answer:-1}"
  case "${answer}" in
    1) printf '180\n' ;;
    2) printf '365\n' ;;
    3) printf '730\n' ;;
    4) printf '1095\n' ;;
    5)
      printf '日数を入力してください [180以上]: ' >/dev/tty
      IFS= read -r custom </dev/tty || true
      custom="${custom:-180}"
      if ! [[ "${custom}" =~ ^[0-9]+$ ]] || [[ "${custom}" -lt 180 ]]; then
        echo "[NG] SQL分析用の履歴日数は 180 以上の整数にしてください。" >&2
        exit 1
      fi
      printf '%s\n' "${custom}"
      ;;
    *)
      echo "[NG] 選択肢は 1, 2, 3, 4, 5 のいずれかです。" >&2
      exit 1
      ;;
  esac
}

resolve_bootstrap_args() {
  BOOTSTRAP_ARGS=()
  if [[ "$#" -gt 0 ]]; then
    BOOTSTRAP_ARGS=("$@")
  fi
  if [[ "$#" -gt 0 ]] && has_arg "--analysis-days" "$@"; then
    return
  fi
  if [[ -n "${BOATRACE_ANALYSIS_DAYS:-}" ]]; then
    if ! [[ "${BOATRACE_ANALYSIS_DAYS}" =~ ^[0-9]+$ ]] || [[ "${BOATRACE_ANALYSIS_DAYS}" -lt 180 ]]; then
      echo "[NG] BOATRACE_ANALYSIS_DAYS は 180 以上の整数にしてください。" >&2
      exit 1
    fi
    BOOTSTRAP_ARGS+=("--analysis-days" "${BOATRACE_ANALYSIS_DAYS}")
    return
  fi
  if [[ -r /dev/tty && -w /dev/tty ]]; then
    local selected_days
    selected_days="$(select_analysis_days)"
    BOOTSTRAP_ARGS+=("--analysis-days" "${selected_days}")
  fi
}

resolve_uv_bin() {
  if [[ -n "${UV_BIN}" && -x "${UV_BIN}" ]]; then
    printf '%s\n' "${UV_BIN}"
    return
  fi
  if command -v uv >/dev/null 2>&1; then
    command -v uv
    return
  fi
  if [[ -x "${UV_INSTALL_DIR}/uv" ]]; then
    printf '%s\n' "${UV_INSTALL_DIR}/uv"
    return
  fi
}

ensure_uv() {
  local resolved
  resolved="$(resolve_uv_bin || true)"
  if [[ -n "${resolved}" ]]; then
    UV_BIN="${resolved}"
    echo "[1/7] Python/依存管理ツール uv を確認"
    echo "     [SKIP] 既にあります: ${UV_BIN}"
    echo
    return
  fi

  if ! command -v curl >/dev/null 2>&1; then
    echo "[NG] curl が見つかりません。" >&2
    print_uv_install_help
    exit 1
  fi

  mkdir -p "${UV_INSTALL_DIR}"
  run_with_spinner \
    "1/7" \
    "Python/依存管理ツール uv を導入" \
    "通常 10秒から1分程度。uv は Python 3.11 の自動取得にも使います" \
    "${LOG_DIR}/01_install_uv.log" \
    env UV_INSTALL_DIR="${UV_INSTALL_DIR}" sh -c 'curl -LsSf https://astral.sh/uv/install.sh | sh'

  resolved="$(resolve_uv_bin || true)"
  if [[ -z "${resolved}" ]]; then
    echo "[NG] uv の導入後に実行ファイルを見つけられませんでした。" >&2
    echo "     ログ: ${LOG_DIR}/01_install_uv.log" >&2
    exit 1
  fi
  UV_BIN="${resolved}"
}

run_with_spinner() {
  local step="$1"
  shift
  local label="$1"
  shift
  local estimate="$1"
  shift
  local log_file="$1"
  shift

  printf '[%s] %s\n' "${step}" "${label}"
  printf '     目安: %s\n' "${estimate}"
  printf '     ログ: %s\n' "${log_file}"

  "$@" >"${log_file}" 2>&1 &
  local pid=$!
  local spin='|/-\'
  local i=0
  local start_ts
  start_ts="$(date +%s)"

  while kill -0 "${pid}" 2>/dev/null; do
    local elapsed
    elapsed="$(( $(date +%s) - start_ts ))"
    printf "\r     [%c] 実行中 %s 経過" "${spin:i++%4:1}" "$(format_elapsed "${elapsed}")"
    sleep 0.1
  done

  wait "${pid}" || {
    local exit_code=$?
    printf "\r     [NG] 失敗しました                         \n"
    echo "     ログ: ${log_file}"
    tail -n 40 "${log_file}" || true
    return "${exit_code}"
  }

  local elapsed
  elapsed="$(( $(date +%s) - start_ts ))"
  printf "\r     [OK] 完了 %s 経過                         \n" "$(format_elapsed "${elapsed}")"
  printf '\n'
}

cd "${ROOT_DIR}"
print_header
resolve_bootstrap_args "$@"
ensure_uv

if [[ ! -d "${VENV_DIR}" ]]; then
  run_with_spinner \
    "2/7" \
    "アプリ専用 Python 3.11 環境を作成" \
    "初回は Python 3.11 runtime の取得を含むため数分かかることがあります" \
    "${LOG_DIR}/02_create_python_env.log" \
    "${UV_BIN}" venv --python 3.11 "${VENV_DIR}"
else
  echo "[2/7] アプリ専用 Python 3.11 環境を作成"
  echo "     [SKIP] 既にあります: ${VENV_DIR}"
  echo
fi

run_with_spinner \
  "3/7" \
  "依存関係をインストール" \
  "初回は数分かかることがあります。DuckDB、LightGBM、rich などをアプリ専用環境へ導入します" \
  "${LOG_DIR}/03_install_requirements.log" \
  "${UV_BIN}" pip install --python "${VENV_DIR}/bin/python" -e .

if ! "${VENV_DIR}/bin/python" -c 'import lightgbm' >/dev/null 2>&1; then
  echo "[NG] LightGBM を読み込めません。"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "macOS では libomp が必要な場合があります: brew install libomp"
  fi
  exit 1
fi

cat <<'EOF'
[4/7-7/7] データ取得、特徴量作成、学習、予測、skill/MCP 導入へ進みます。
     ここからは画面に全体進捗、ステージ別進捗、現在処理中の内容を表示します。
     特に「特徴量作成と学習」は履歴集計と LightGBM 学習を行うため時間がかかります。

EOF
if [[ "${#BOOTSTRAP_ARGS[@]}" -gt 0 ]]; then
  exec "${VENV_DIR}/bin/python" "${ROOT_DIR}/scripts/boatrace_bootstrap.py" "${BOOTSTRAP_ARGS[@]}"
else
  exec "${VENV_DIR}/bin/python" "${ROOT_DIR}/scripts/boatrace_bootstrap.py"
fi
