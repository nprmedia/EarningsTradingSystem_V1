#!/usr/bin/env bash
set -euo pipefail
python - <<'PY'
from ets.scripts.build_universe import build_universe
build_universe()
PY
