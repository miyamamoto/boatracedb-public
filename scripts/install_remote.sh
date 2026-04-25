#!/usr/bin/env bash
set -euo pipefail

REPO_SLUG="${BOATRACE_REPO_SLUG:-miyamamoto/boatracedb-public}"
BRANCH="${BOATRACE_BRANCH:-main}"
INSTALL_DIR="${BOATRACE_INSTALL_DIR:-${HOME}/boatracedb}"
ARCHIVE_URL="${BOATRACE_ARCHIVE_URL:-https://github.com/${REPO_SLUG}/archive/refs/heads/${BRANCH}.tar.gz}"
BOOTSTRAP_ARGS=("$@")

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

has_bootstrap_arg() {
  local name="$1"
  shift
  for arg in "$@"; do
    if [[ "${arg}" == "${name}" || "${arg}" == "${name}="* ]]; then
      return 0
    fi
  done
  return 1
}

need_command bash
need_command curl
need_command tar
need_command mktemp
need_command cp

cat <<'EOF'
BoatRace Local Predictor remote installer

このコマンドはアプリ本体だけを GitHub から取得し、~/boatracedb に配置します。
git や Python を事前に入れておく必要はありません。

データ取得の考え方:
  - 既存の DuckDB と data/comprehensive_cache は削除しません。
  - fetch はまずローカル cache を読み、cache にある日付はリモート取得しません。
  - 初回インストールでは、必要な不足データだけリモート取得します。
  - 既存インストールの更新では、明示指定がなければリモートデータ取得を避けます。
  - 更新時に不足データも取りたい場合は --download-missing を付けてください。

bootstrap では必要に応じて、データ確認、特徴量作成、モデル学習、予測、skill/MCP 導入を行います。
初回は特徴量作成と学習に時間がかかるため、進捗表示を見ながら待ってください。

免責:
  このシステムの予測、買い目候補、SQL分析結果、番組表、説明文は参考情報です。
  開発者および配布者は出力の正確性や利用結果について一切の責任を負いません。
  回収率はオッズ、購入点数、資金配分、直前情報に左右され、プラス収支は保証されません。
  予測はあくまでレースを楽しむための材料です。
  舟券購入やその他の判断は、必ず利用者自身の責任で行ってください。

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

EXISTING_INSTALL=0
if [[ -d "${INSTALL_DIR}" ]]; then
  EXISTING_INSTALL=1
  log "既存インストールを更新します: ${INSTALL_DIR}"
  log "既存の data/ と models/ と .venv は保持します"
  if [[ "${BOATRACE_UPDATE_DOWNLOAD_MISSING:-0}" != "1" ]] \
    && ! has_bootstrap_arg "--download-missing" "${BOOTSTRAP_ARGS[@]}" \
    && ! has_bootstrap_arg "--no-download-missing" "${BOOTSTRAP_ARGS[@]}" \
    && ! has_bootstrap_arg "--cache-only" "${BOOTSTRAP_ARGS[@]}"; then
    BOOTSTRAP_ARGS+=("--no-download-missing")
    log "更新モード: リモートデータ取得を避け、ローカル cache/DuckDB を優先します"
    log "不足データも取得したい場合は、次回 --download-missing を付けてください"
  fi
else
  log "インストール先を作成します: ${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"
  log "初回モード: cache に無い必要データは不足分だけリモート取得します"
fi

run_with_spinner "アプリファイルを配置" cp -R "${EXTRACTED_DIR}/." "${INSTALL_DIR}/"

if [[ "${BOATRACE_SKIP_BOOTSTRAP:-}" == "1" ]]; then
  log "BOATRACE_SKIP_BOOTSTRAP=1 のため bootstrap は実行しません"
  exit 0
fi

log "ローカル予測環境を初期化します"
if [[ "${EXISTING_INSTALL}" == "1" ]]; then
  log "bootstrap 引数: ${BOOTSTRAP_ARGS[*]:-(なし)}"
fi
exec bash "${INSTALL_DIR}/scripts/install_boatrace_local.sh" "${BOOTSTRAP_ARGS[@]}"
