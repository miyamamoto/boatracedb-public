#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${BOATRACE_REPO_URL:-https://github.com/miyamamoto/boatracedb-public.git}"
BRANCH="${BOATRACE_BRANCH:-main}"
INSTALL_DIR="${BOATRACE_INSTALL_DIR:-${HOME}/boatracedb}"

log() {
  printf '[boatrace] %s\n' "$1"
}

need_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "必要なコマンドが見つかりません: $1" >&2
    exit 1
  fi
}

need_command git
need_command bash

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
  git -C "${INSTALL_DIR}" fetch origin "${BRANCH}"
  git -C "${INSTALL_DIR}" switch "${BRANCH}"
  git -C "${INSTALL_DIR}" pull --ff-only origin "${BRANCH}"
elif [[ -e "${INSTALL_DIR}" ]]; then
  echo "インストール先が既に存在しますが Git リポジトリではありません: ${INSTALL_DIR}" >&2
  echo "別の場所に入れる場合は BOATRACE_INSTALL_DIR=/path/to/dir を指定してください。" >&2
  exit 1
else
  log "リポジトリを取得します: ${REPO_URL} (${BRANCH})"
  git clone --branch "${BRANCH}" "${REPO_URL}" "${INSTALL_DIR}"
fi

log "ローカル予測環境を初期化します"
exec bash "${INSTALL_DIR}/scripts/install_boatrace_local.sh" "$@"
