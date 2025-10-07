#!/usr/bin/env bash
set -euo pipefail
python - <<'PY'
from ets.scripts.prefetch_daily import main
main(days=7)
PY
