#!/usr/bin/env bash
set -euo pipefail

export PYTHONHASHSEED=0 TZ=UTC LC_ALL=C.UTF-8 LANG=C.UTF-8 MPLBACKEND=Agg
export ETS_SEED=4242 ETS_OFFLINE=1 ETS_STRICT_DETERMINISM=1

mkdir -p reports/coverage reports/junit out

ruff check .
black --check .

pytest -m "integration" \
  --cov=src/ets \
  --cov-report=xml:reports/coverage/coverage-integration.xml \
  --cov-report=term-missing \
  --junitxml=reports/junit/junit-integration.xml

python - <<'PY'
import xml.etree.ElementTree as ET, sys
root = ET.parse("reports/coverage/coverage-integration.xml").getroot()
rate = float(root.attrib["line-rate"]) * 100.0
print(f"Integration coverage: {rate:.2f}%")
sys.exit(0 if rate >= 70 else 1)
PY
