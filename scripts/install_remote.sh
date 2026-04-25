#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${BOATRACE_REPO_URL:-https://github.com/miyamamoto/boatracedb-public.git}"
BRANCH="${BOATRACE_BRANCH:-main}"
INSTALL_DIR="${BOATRACE_INSTALL_DIR:-${HOME}/boatracedb}"

log() {
  printf '[boatrace] %s\n' "$1"
}

format_elapsed() {
  local seconds="$1"
  printf "%02d:%02d" "$((seconds / 60))" "$((seconds % 60))"
}

run_with_spinner() {
  local label="$1"
  shift
  local spin='|/-\'
  local i=0
  local start_ts
  start_ts="$(date +%s)"

  "$@" &
  local pid=$!
  while kill -0 "${pid}" 2>/dev/null; do
    local elapsed
    elapsed="$(( $(date +%s) - start_ts ))"
    printf "\r[%c] %s 経過 %s" "${spin:i++%4:1}" "${label}" "$(format_elapsed "${elapsed}")"
    sleep 0.1
  done

  wait "${pid}" || {
    local exit_code=$?
    printf "\r[NG] %s                         \n" "${label}"
    return "${exit_code}"
  }
  local elapsed
  elapsed="$(( $(date +%s) - start_ts ))"
  printf "\r[OK] %s 完了 %s                         \n" "${label}" "$(format_elapsed "${elapsed}")"
}

need_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "必要なコマンドが見つかりません: $1" >&2
    exit 1
  fi
}

need_command git
need_command bash

cat <<'EOF'
BoatRace Local Predictor remote installer

このコマンドはリポジトリ取得後、ローカル bootstrap を起動します。
bootstrap ではデータ取得、特徴量作成、モデル学習、予測、skill 導入まで実行します。
初回は特徴量作成と学習に時間がかかるため、進捗表示を見ながら待ってください。

EOF

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "Python 3.11 以上が必要です。" >&2
  exit 1
fi

if ! "${PYTHON_BIN}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1; then
  echo "Python 3.11 以上が必要です。" >&2
  exit 1
fi

if [[ -d "${INSTALL_DIR}/.git" ]]; then
  log "既存リポジトリを更新します: ${INSTALL_DIR}"
  run_with_spinner "リモート更新を確認" git -C "${INSTALL_DIR}" fetch origin "${BRANCH}"
  run_with_spinner "ブランチを切り替え" git -C "${INSTALL_DIR}" switch "${BRANCH}"
  run_with_spinner "最新版を取得" git -C "${INSTALL_DIR}" pull --ff-only origin "${BRANCH}"
elif [[ -e "${INSTALL_DIR}" ]]; then
  echo "インストール先が既に存在しますが Git リポジトリではありません: ${INSTALL_DIR}" >&2
  echo "別の場所に入れる場合は BOATRACE_INSTALL_DIR=/path/to/dir を指定してください。" >&2
  exit 1
else
  log "リポジトリを取得します: ${REPO_URL} (${BRANCH})"
  run_with_spinner "リポジトリを clone" git clone --branch "${BRANCH}" "${REPO_URL}" "${INSTALL_DIR}"
fi

log "ローカル予測環境を初期化します"
exec bash "${INSTALL_DIR}/scripts/install_boatrace_local.sh" "$@"
