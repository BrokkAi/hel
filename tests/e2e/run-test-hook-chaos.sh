#!/usr/bin/env bash
set -euo pipefail

if [[ ${HEL_CHAOS_ISOLATED:-} != 1 ]]; then
    echo "refusing to run crash hooks without HEL_CHAOS_ISOLATED=1" >&2
    exit 2
fi
if [[ $# -lt 1 ]]; then
    echo "usage: $0 /path/to/test-hooks-enabled-hel [--hook NAME] [--seed N]" >&2
    exit 2
fi

hel_binary=$1
shift
[[ -x $hel_binary ]] || { echo "Hel binary is not executable: $hel_binary" >&2; exit 2; }
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 "$script_dir/test_hook_chaos.py" "$@" "$hel_binary"
