#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR/src"
PYTHONPATH="$ROOT_DIR/src" "$ROOT_DIR/.venv/bin/pytest" tests
