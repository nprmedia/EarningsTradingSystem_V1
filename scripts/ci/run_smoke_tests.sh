#!/usr/bin/env bash
set -euo pipefail

export PYTHONHASHSEED=0 TZ=UTC LC_ALL=C.UTF-8 LANG=C.UTF-8 MPLBACKEND=Agg
export ETS_SEED=1337 ETS_OFFLINE=1 ETS_STRICT_DETERMINISM=1

mkdir -p reports/coverage reports/junit

ruff check .
black --check .

pytest -m "smoke" \
  --cov=src/ets \
  --cov-report=xml:reports/coverage/coverage-smoke.xml \
  --cov-report=term-missing \
  --junitxml=reports/junit/junit-smoke.xml

python - <<'PY'
import xml.etree.ElementTree as ET, sys
root = ET.parse("reports/coverage/coverage-smoke.xml").getroot()
rate = float(root.attrib["line-rate"]) * 100.0
print(f"Smoke coverage: {rate:.2f}%")
sys.exit(0 if rate >= 65 else 1)
PY
