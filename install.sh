#!/bin/sh
# Install a published Hel release without requiring Rust or a source checkout.
set -eu

SCRIPT_VERSION="1.0.0"
OWNER="${HEL_GITHUB_OWNER:-BrokkAi}"
REPOSITORY="${HEL_GITHUB_REPOSITORY:-hel}"
: "${HOME:?HOME must be set}"
PREFIX="${HEL_PREFIX:-$HOME/.local}"
VERSION="${HEL_VERSION:-}"
TMP_DIR=""
TARGET=""
BIN_DIR=""

log() {
  printf 'hel-installer: %s\n' "$*"
}

die() {
  printf 'hel-installer: error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Install the latest Hel release.

Usage:
  curl -fsSL https://raw.githubusercontent.com/BrokkAi/hel/master/install.sh | sh
  curl -fsSL https://raw.githubusercontent.com/BrokkAi/hel/master/install.sh | sh -s -- --prefix /usr/local

Options:
  --prefix DIRECTORY  Install binaries in DIRECTORY/bin (default: ~/.local/bin).
  --version TAG       Install a specific release tag instead of the latest release.
  -h, --help          Show this help.

Environment:
  HEL_PREFIX              Same as --prefix.
  HEL_VERSION             Same as --version, for example v0.1.0.
  HEL_GITHUB_OWNER        GitHub owner to download from (default: BrokkAi).
  HEL_GITHUB_REPOSITORY   GitHub repository to download from (default: hel).
  GITHUB_TOKEN            Optional token for GitHub download rate limits.
EOF
}

cleanup() {
  if [ -n "${TMP_DIR:-}" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

download_file() {
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    curl -fsSL --retry 3 --retry-delay 1 \
      -H "Authorization: Bearer ${GITHUB_TOKEN}" \
      -o "$2" "$1"
  else
    curl -fsSL --retry 3 --retry-delay 1 -o "$2" "$1"
  fi
}

detect_platform() {
  os_name="$(uname -s)"
  arch_name="$(uname -m)"

  case "$os_name/$arch_name" in
    Darwin/arm64 | Darwin/aarch64)
      TARGET="aarch64-apple-darwin"
      ;;
    Linux/x86_64 | Linux/amd64)
      TARGET="x86_64-unknown-linux-gnu"
      ;;
    Linux/arm64 | Linux/aarch64)
      TARGET="aarch64-unknown-linux-gnu"
      ;;
    Darwin/*)
      die "unsupported macOS architecture: ${arch_name}; Apple silicon is currently supported"
      ;;
    Linux/*)
      die "unsupported Linux architecture: ${arch_name}"
      ;;
    *)
      die "unsupported platform: ${os_name}/${arch_name}"
      ;;
  esac
}

validate_release_tag() {
  if [ -z "$VERSION" ]; then
    return
  fi

  case "$VERSION" in
    *[!A-Za-z0-9._-]*) die "invalid release tag: ${VERSION}" ;;
  esac
}

release_download_base() {
  if [ -n "$VERSION" ]; then
    printf 'https://github.com/%s/%s/releases/download/%s\n' \
      "$OWNER" "$REPOSITORY" "$VERSION"
  else
    printf 'https://github.com/%s/%s/releases/latest/download\n' \
      "$OWNER" "$REPOSITORY"
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

checksum_for_asset() {
  awk -v asset="$2" '
    NF >= 2 && ($2 == asset || $2 == "*" asset) &&
    length($1) == 64 && $1 ~ /^[[:xdigit:]]+$/ {
      print tolower($1)
      exit
    }
  ' "$1"
}

install_binary() {
  source_path="$1"
  binary_name="$2"
  install -m 0755 "$source_path" "$BIN_DIR/$binary_name" ||
    die "could not install ${binary_name} to ${BIN_DIR}"
}

path_includes_bin_dir() {
  case ":${PATH:-}:" in
    *":${BIN_DIR}:"*) return 0 ;;
    *) return 1 ;;
  esac
}

print_next_steps() {
  hel_command="hel"
  if ! path_includes_bin_dir; then
    hel_command="$BIN_DIR/hel"
    log "${BIN_DIR} is not on PATH; add it to your shell profile to use hel directly"
  fi

  printf '\nNext steps:\n'
  printf '  %s doctor\n' "$hel_command"
  printf '  %s setup\n' "$hel_command"
}

parse_arguments() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --prefix)
        [ "$#" -ge 2 ] || die "--prefix requires a directory"
        PREFIX="$2"
        shift 2
        ;;
      --prefix=*)
        PREFIX="${1#--prefix=}"
        shift
        ;;
      --version)
        [ "$#" -ge 2 ] || die "--version requires a tag"
        VERSION="$2"
        shift 2
        ;;
      --version=*)
        VERSION="${1#--version=}"
        shift
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

main() {
  parse_arguments "$@"
  [ -n "$PREFIX" ] || die "installation prefix must not be empty"
  validate_release_tag

  require_command awk
  require_command curl
  require_command install
  require_command mktemp
  require_command tar
  require_command tr
  if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
    die "required command not found: sha256sum or shasum"
  fi

  detect_platform
  BIN_DIR="$PREFIX/bin"
  if ! mkdir -p "$BIN_DIR"; then
    die "could not create ${BIN_DIR}; choose a writable --prefix"
  fi

  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hel-installer.XXXXXX")" ||
    die "could not create a temporary directory"
  trap cleanup 0 HUP INT TERM

  asset_name="hel-${TARGET}.tar.gz"
  archive_path="$TMP_DIR/$asset_name"
  checksum_path="$TMP_DIR/$asset_name.sha256"
  download_base="$(release_download_base)"

  log "downloading ${VERSION:-latest} Hel release for ${TARGET}"
  download_file "$download_base/$asset_name" "$archive_path"
  download_file "$download_base/$asset_name.sha256" "$checksum_path"

  expected_checksum="$(checksum_for_asset "$checksum_path" "$asset_name")"
  [ -n "$expected_checksum" ] ||
    die "published checksum did not contain a valid entry for ${asset_name}"
  actual_checksum="$(sha256_file "$archive_path" | tr 'A-F' 'a-f')"
  [ "$expected_checksum" = "$actual_checksum" ] ||
    die "checksum mismatch for ${asset_name}"
  log "verified ${asset_name}"

  extract_dir="$TMP_DIR/extract"
  mkdir "$extract_dir"
  tar -xzf "$archive_path" -C "$extract_dir"
  archive_root="$extract_dir/hel-$TARGET"
  [ -f "$archive_root/hel" ] || die "archive did not contain hel"
  [ -f "$archive_root/hel-worker-x86_64-unknown-linux-musl" ] ||
    die "archive did not contain the x86_64 Linux worker"
  [ -f "$archive_root/hel-worker-aarch64-unknown-linux-musl" ] ||
    die "archive did not contain the aarch64 Linux worker"

  install_binary "$archive_root/hel" hel
  install_binary "$archive_root/hel-worker-x86_64-unknown-linux-musl" \
    hel-worker-x86_64-unknown-linux-musl
  install_binary "$archive_root/hel-worker-aarch64-unknown-linux-musl" \
    hel-worker-aarch64-unknown-linux-musl

  log "installed Hel to ${BIN_DIR} (installer ${SCRIPT_VERSION})"
  print_next_steps
}

main "$@"
