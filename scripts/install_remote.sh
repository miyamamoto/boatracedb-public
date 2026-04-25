#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${BOATRACE_REPO_SLUG:-miyamamoto/boatracedb-public}"
BRANCH="${BOATRACE_BRANCH:-main}"
INSTALL_DIR="${BOATRACE_INSTALL_DIR:-${HOME}/boatracedb}"
ARCHIVE_URL="${BOATRACE_ARCHIVE_URL:-https://github.com/${REPO_SLUG}/archive/refs/heads/${BRANCH}.tar.gz}"

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
    echo "[NG] 必要なコマンドが見つかりません: $1" >&2
    exit 1
  fi
}

need_command bash
need_command curl
need_command tar
need_command mktemp
need_command cp

cat <<'EOF'
BoatRace Local Predictor remote installer

このコマンドはアプリ一式をダウンロードして展開し、ローカル bootstrap を起動します。
git や Python を事前に入れておく必要はありません。
bootstrap ではデータ取得、特徴量作成、モデル学習、予測、skill/MCP 導入まで実行します。
初回は特徴量作成と学習に時間がかかるため、進捗表示を見ながら待ってください。

EOF

TMP_PARENT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "${TMP_PARENT%/}/boatrace-installer.XXXXXX")"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

if [[ -e "${INSTALL_DIR}" && ! -d "${INSTALL_DIR}" ]]; then
  echo "インストール先が既に存在しますがディレクトリではありません: ${INSTALL_DIR}" >&2
  echo "別の場所に入れる場合は BOATRACE_INSTALL_DIR=/path/to/dir を指定してください。" >&2
  exit 1
fi

log "アプリ一式をダウンロードします: ${ARCHIVE_URL}"
run_with_spinner "パッケージをダウンロード" curl -fsSL "${ARCHIVE_URL}" -o "${TMP_DIR}/boatrace.tar.gz"
run_with_spinner "パッケージを展開" tar -xzf "${TMP_DIR}/boatrace.tar.gz" -C "${TMP_DIR}"

EXTRACTED_DIR="$(find "${TMP_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [[ -z "${EXTRACTED_DIR}" || ! -d "${EXTRACTED_DIR}" ]]; then
  echo "展開したパッケージの中身を確認できませんでした。" >&2
  exit 1
fi

if [[ -d "${INSTALL_DIR}" ]]; then
  log "既存インストールを更新します: ${INSTALL_DIR}"
else
  log "インストール先を作成します: ${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"
fi

run_with_spinner "アプリファイルを配置" cp -R "${EXTRACTED_DIR}/." "${INSTALL_DIR}/"

if [[ "${BOATRACE_SKIP_BOOTSTRAP:-}" == "1" ]]; then
  log "BOATRACE_SKIP_BOOTSTRAP=1 のため bootstrap は実行しません"
  exit 0
fi

log "ローカル予測環境を初期化します"
exec bash "${INSTALL_DIR}/scripts/install_boatrace_local.sh" "$@"
