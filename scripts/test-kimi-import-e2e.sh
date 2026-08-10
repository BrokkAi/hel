#!/usr/bin/env bash
# Run a genuine Kimi Code import/resume cycle on a host with Podman.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
kimi_home=${KIMI_CODE_HOME:-"$HOME/.kimi-code"}
test_root=${HEL_IMPORT_E2E_KIMI_ROOT:-"${XDG_STATE_HOME:-"$HOME/.local/state"}/hel/import-e2e/kimi-native"}

if [[ ! -d "$kimi_home" ]]; then
    echo "Kimi Code home does not exist: $kimi_home" >&2
    exit 1
fi

export KIMI_CODE_HOME="$kimi_home"
export HEL_IMPORT_E2E_ROOT="$test_root"
export HEL_IMPORT_E2E_KIMI_SESSION="${HEL_IMPORT_E2E_KIMI_SESSION:-session_1b6c3192-2480-48e0-8f49-4b8a1572f5b2}"
export HEL_IMPORT_E2E_KIMI_REPOSITORY="${HEL_IMPORT_E2E_KIMI_REPOSITORY:-MoonshotAI/kimi-code}"
export HEL_IMPORT_E2E_IMAGE="${HEL_IMPORT_E2E_IMAGE:-localhost/hel/agent-dev:latest}"
# Keep the test's imported state and archive separate from the user's Hel data.
export HEL_CONFIG_DIR="$test_root/config/hel"
export HEL_DATA_DIR="$test_root/data/hel"
export HEL_WORKER_BINARY="${HEL_WORKER_BINARY:-$repo_root/target/x86_64-unknown-linux-musl/debug/hel}"

mkdir -p "$test_root"
cd "$repo_root"
if [[ ! -x "$HEL_WORKER_BINARY" ]]; then
    cargo build --target x86_64-unknown-linux-musl
fi
cargo test --test import_e2e imported_kimi_session_resumes_natively -- --ignored --nocapture
