#!/usr/bin/env bash
set -euo pipefail

minutes=
seed=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --minutes)
            [[ $# -ge 2 ]] || { echo "--minutes requires a value" >&2; exit 2; }
            minutes=$2
            shift 2
            ;;
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

if [[ ! $minutes =~ ^[0-9]+$ ]] || (( minutes < 1 || minutes > 60 )); then
    echo "minutes must be an integer from 1 through 60" >&2
    exit 2
fi
if [[ ! $seed =~ ^[0-9]+$ ]] || (( seed > 2147483647 )); then
    echo "seed must be an integer from 0 through 2147483647" >&2
    exit 2
fi
if [[ $# -ne 1 || ! -x $1 ]]; then
    echo "usage: $0 --minutes 30 --seed NUMBER /path/to/hel" >&2
    exit 2
fi

hel_binary=$(realpath -- "$1")
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
started=$(date +%s)
deadline=$((started + minutes * 60))
iteration=0
while (( $(date +%s) < deadline )); do
    replay_seed=$((seed + iteration))
    if (( replay_seed > 2147483647 )); then
        replay_seed=$((replay_seed - 2147483648))
    fi
    echo "soak: iteration=$iteration seed=$replay_seed"
    "$script_dir/run-reliability.sh" \
        --scenario multi-client-happy-path \
        --seed "$replay_seed" \
        "$hel_binary"
    iteration=$((iteration + 1))
done

elapsed=$(( $(date +%s) - started ))
echo "soak: passed iterations=$iteration elapsed_seconds=$elapsed first_seed=$seed"
