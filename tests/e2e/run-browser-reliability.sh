#!/usr/bin/env bash
set -euo pipefail

seed=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seed)
            [[ $# -ge 2 ]] || { echo "--seed requires a value" >&2; exit 2; }
            seed=$2
            shift 2
            ;;
        --)
            shift
            break
            ;;
        -*)
            echo "unknown option: $1" >&2
            exit 2
            ;;
        *)
            break
            ;;
    esac
done

if [[ -z $seed || $# -ne 1 ]]; then
    echo "usage: $0 --seed NUMBER /path/to/hel" >&2
    exit 2
fi
if [[ ! $seed =~ ^[0-9]+$ ]]; then
    echo "seed must be an unsigned integer: $seed" >&2
    exit 2
fi
hel_binary=$(realpath -- "$1")
if [[ ! -x $hel_binary ]]; then
    echo "Hel binary is not executable: $hel_binary" >&2
    exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ ! -x $script_dir/web/node_modules/.bin/playwright ]]; then
    echo "Playwright is not installed; run npm ci in tests/e2e/web" >&2
    exit 2
fi
exec python3 "$script_dir/browser_lab.py" --seed "$seed" --hel "$hel_binary"
