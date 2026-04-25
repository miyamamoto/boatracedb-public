#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
PYTHON_BIN="${PYTHON:-python3}"
LOG_DIR="${ROOT_DIR}/logs/bootstrap-install"

mkdir -p "${LOG_DIR}"

format_elapsed() {
  local seconds="$1"
  printf "%02d:%02d" "$((seconds / 60))" "$((seconds % 60))"
}

print_header() {
  cat <<'EOF'
BoatRace Local Predictor installer

これから次の順でセットアップします。
  1. Python 仮想環境を作成
  2. pip と依存関係を導入
  3. 過去データを取得
  4. 特徴量を作成してローカルモデルを学習
  5. 対象日の予測を生成
  6. Codex / Claude Code skill を導入

注意:
  - 初回はデータ取得、特徴量作成、LightGBM 学習に時間がかかります。
  - SQL分析用に投入する過去データ量もこのあと確認します。
  - 90日学習では端末やネットワークにより数分から十数分程度かかることがあります。
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

resolve_bootstrap_args() {
  BOOTSTRAP_ARGS=("$@")
  if has_arg "--analysis-days" "${BOOTSTRAP_ARGS[@]}"; then
    return
  fi
  if [[ -n "${BOATRACE_ANALYSIS_DAYS:-}" ]]; then
    BOOTSTRAP_ARGS+=("--analysis-days" "${BOATRACE_ANALYSIS_DAYS}")
    return
  fi
  if [[ -t 0 ]]; then
    local answer
    printf 'SQL分析用にDuckDBへ投入する過去実績は何日分にしますか？ [180]: '
    read -r answer || true
    answer="${answer:-180}"
    BOOTSTRAP_ARGS+=("--analysis-days" "${answer}")
  fi
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

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "python3 が見つかりません。Python 3.11 以上を用意してください。"
  exit 1
fi

if ! "${PYTHON_BIN}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1; then
  echo "Python 3.11 以上が必要です。"
  exit 1
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  run_with_spinner \
    "1/6" \
    "Python 仮想環境を作成" \
    "通常 10秒から1分程度" \
    "${LOG_DIR}/01_create_venv.log" \
    "${PYTHON_BIN}" -m venv "${VENV_DIR}"
else
  echo "[1/6] Python 仮想環境を作成"
  echo "     [SKIP] 既にあります: ${VENV_DIR}"
  echo
fi

run_with_spinner \
  "2/6" \
  "pip を更新" \
  "通常 10秒から1分程度" \
  "${LOG_DIR}/02_upgrade_pip.log" \
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip

run_with_spinner \
  "3/6" \
  "依存関係をインストール" \
  "初回は数分かかることがあります。DuckDB、LightGBM、rich などを導入します" \
  "${LOG_DIR}/03_install_requirements.log" \
  "${VENV_DIR}/bin/python" -m pip install -e .

if ! "${VENV_DIR}/bin/python" -c 'import lightgbm' >/dev/null 2>&1; then
  echo "[NG] LightGBM を読み込めません。"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "macOS では libomp が必要な場合があります: brew install libomp"
  fi
  exit 1
fi

cat <<'EOF'
[4/6-6/6] データ取得、特徴量作成、学習、予測、skill 導入へ進みます。
     ここからは画面に全体進捗、ステージ別進捗、現在処理中の内容を表示します。
     特に「特徴量作成と学習」は履歴集計と LightGBM 学習を行うため時間がかかります。

EOF
exec "${VENV_DIR}/bin/python" "${ROOT_DIR}/scripts/boatrace_bootstrap.py" "${BOOTSTRAP_ARGS[@]}"
