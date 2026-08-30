#!/usr/bin/env python3
"""Prepare, but do not start, a disposable Hel lab for tmux exploration."""

from __future__ import annotations

import argparse
import json
import pathlib
import shlex

from reliability_lab import Lab


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--hel", required=True, type=pathlib.Path)
    args = parser.parse_args()

    lab = Lab(args.hel, "luna-manual", args.seed)
    port = lab.prepare()
    values = {
        "HEL_CONFIG_DIR": str(lab.config),
        "HEL_DATA_DIR": str(lab.data),
        "HEL_CHAOS_ISOLATED": "1",
        "RUST_LOG": "hel=debug,hel_cli=debug",
        "HEL_LUNA_ARTIFACTS": str(lab.root),
        "HEL_LUNA_RUNTIME_ROOT": str(lab.runtime_root),
        "HEL_LUNA_PORT": str(port),
        "HEL_LUNA_BINARY": str(lab.hel),
    }
    environment_file = lab.root / "luna-env.sh"
    environment_file.write_text(
        "\n".join(f"export {key}={shlex.quote(value)}" for key, value in values.items()) + "\n"
    )
    (lab.root / "luna-lab.json").write_text(
        json.dumps({"seed": args.seed, "port": port, **values}, indent=2, sort_keys=True) + "\n"
    )
    print(f"artifacts={lab.root}")
    print(f"runtime={lab.runtime_root}")
    print(f"source {shlex.quote(str(environment_file))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
