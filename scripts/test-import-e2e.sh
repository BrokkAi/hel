#!/usr/bin/env bash
# Run a genuine Claude Code import/resume cycle on a host with Podman.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
real_claude_home=${CLAUDE_CONFIG_DIR:-"$HOME/.claude"}
test_root=${HEL_IMPORT_E2E_ROOT:-"${XDG_STATE_HOME:-"$HOME/.local/state"}/hel/import-e2e"}

if [[ ! -d "$real_claude_home" ]]; then
    echo "Claude home does not exist: $real_claude_home" >&2
    exit 1
fi

export CLAUDE_CONFIG_DIR="$real_claude_home"
export HEL_IMPORT_E2E_ROOT="$test_root"
export HEL_IMPORT_E2E_REPOSITORY="${HEL_IMPORT_E2E_REPOSITORY:-BrokkAi/hel}"
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
cargo test --test import_e2e imported_claude_session_resumes_natively -- --ignored --nocapture
