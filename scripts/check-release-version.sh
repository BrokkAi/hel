#!/usr/bin/env bash
set -euo pipefail

release_tag="${1:-}"
if [[ ! "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 vX.Y.Z" >&2
  exit 2
fi

release_version="${release_tag#v}"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

metadata="$(cargo metadata --locked --no-deps --format-version 1)"
published_crates=(hel-core hel-tui hel-cli hel-voice-worker)

for crate in "${published_crates[@]}"; do
  matches="$(jq --arg crate "$crate" '[.packages[] | select(.name == $crate)] | length' <<<"$metadata")"
  if [[ "$matches" != 1 ]]; then
    echo "expected exactly one workspace package named $crate, found $matches" >&2
    exit 1
  fi

  crate_version="$(jq -r --arg crate "$crate" '.packages[] | select(.name == $crate) | .version' <<<"$metadata")"
  if [[ "$crate_version" != "$release_version" ]]; then
    echo "release tag $release_tag does not match $crate version $crate_version" >&2
    exit 1
  fi
done

internal_dependencies=(
  "hel-tui hel-core"
  "hel-cli hel-core"
  "hel-cli hel-tui"
)
expected_requirement="^$release_version"

for relationship in "${internal_dependencies[@]}"; do
  read -r owner dependency <<<"$relationship"
  matches="$(jq --arg owner "$owner" --arg dependency "$dependency" \
    '[.packages[] | select(.name == $owner) | .dependencies[] | select(.name == $dependency)] | length' \
    <<<"$metadata")"
  if [[ "$matches" != 1 ]]; then
    echo "expected exactly one $owner dependency on $dependency, found $matches" >&2
    exit 1
  fi

  requirement="$(jq -r --arg owner "$owner" --arg dependency "$dependency" \
    '.packages[] | select(.name == $owner) | .dependencies[] | select(.name == $dependency) | .req' \
    <<<"$metadata")"
  if [[ "$requirement" != "$expected_requirement" ]]; then
    echo "$owner dependency on $dependency uses $requirement; expected $expected_requirement" >&2
    exit 1
  fi
done

echo "release tag $release_tag matches all workspace package and internal dependency versions"
