#!/usr/bin/env bash
# Build the hub Lambda bundle for the arm64 python3.12 runtime without Docker
# (PLAN.md, stage 7 Q10).
#
# The project itself goes in with --no-deps so that agent_bus has its
# dist-info: agent_bus/__init__.py reads its version from package metadata.
# pydantic comes from manylinux wheels for the Lambda platform; boto3 is
# already in the runtime.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
out="$here/build"

rm -rf "$out"
mkdir -p "$out"
uv pip install --quiet --target "$out" \
    --python-platform aarch64-manylinux2014 --python-version 3.12 --only-binary :all: \
    "pydantic>=2.7"
uv pip install --quiet --target "$out" --no-deps \
    --python-platform aarch64-manylinux2014 --python-version 3.12 \
    "$root"
du -sh "$out"
