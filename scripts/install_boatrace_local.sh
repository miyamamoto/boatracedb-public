#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
PYTHON_BIN="${PYTHON:-python3}"
LOG_DIR="${ROOT_DIR}/logs/bootstrap-install"

mkdir -p "${LOG_DIR}"

run_with_spinner() {
  local label="$1"
  shift
  local log_file="$1"
  shift

  "$@" >"${log_file}" 2>&1 &
  local pid=$!
  local spin='|/-\'
  local i=0
  local start_ts
  start_ts="$(date +%s)"

  while kill -0 "${pid}" 2>/dev/null; do
    local elapsed
    elapsed="$(( $(date +%s) - start_ts ))"
    printf "\r[%c] %s (%ss)" "${spin:i++%4:1}" "${label}" "${elapsed}"
    sleep 0.1
  done

  wait "${pid}" || {
    local exit_code=$?
    printf "\r[NG] %s\n" "${label}"
    echo "ログ: ${log_file}"
    tail -n 40 "${log_file}" || true
    return "${exit_code}"
  }

  printf "\r[OK] %s\n" "${label}"
}

cd "${ROOT_DIR}"

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
    "仮想環境を作成中" \
    "${LOG_DIR}/01_create_venv.log" \
    "${PYTHON_BIN}" -m venv "${VENV_DIR}"
else
  echo "[SKIP] 仮想環境は既にあります: ${VENV_DIR}"
fi

run_with_spinner \
  "pip を更新中" \
  "${LOG_DIR}/02_upgrade_pip.log" \
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip

run_with_spinner \
  "依存関係をインストール中" \
  "${LOG_DIR}/03_install_requirements.log" \
  "${VENV_DIR}/bin/python" -m pip install -e .

if ! "${VENV_DIR}/bin/python" -c 'import lightgbm' >/dev/null 2>&1; then
  echo "[NG] LightGBM を読み込めません。"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "macOS では libomp が必要な場合があります: brew install libomp"
  fi
  exit 1
fi

exec "${VENV_DIR}/bin/python" "${ROOT_DIR}/scripts/boatrace_bootstrap.py" "$@"
