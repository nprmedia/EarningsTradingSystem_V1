#!/usr/bin/env bash
# ===============================================================
# NPR Media — Earnings Trading System
# Developer Setup Helper
# ---------------------------------------------------------------
# Creates or activates a local virtual environment (.venv),
# installs all core dependencies, and verifies readiness.
# ===============================================================

set -e

VENV_DIR=".venv"

# 1. Create venv if missing
if [ ! -d "$VENV_DIR" ]; then
  echo "[INFO] Creating virtual environment in $VENV_DIR ..."
  python3 -m venv "$VENV_DIR"
fi

# 2. Activate environment
if [ -f "$VENV_DIR/bin/activate" ]; then
  source "$VENV_DIR/bin/activate"
else
  echo "[ERROR] Could not activate $VENV_DIR/bin/activate"
  exit 1
fi

# 3. Upgrade pip and install dependencies
echo "[INFO] Installing core dependencies ..."
pip install -U pip wheel setuptools
pip install -U pandas psutil pytest black ruff

# 4. Verify installs
python - <<'PY'
import sys, pandas, psutil
print(f"[OK] Python {sys.version.split()[0]} — pandas {pandas.__version__}, psutil {psutil.__version__}")
PY

echo "[SUCCESS] Virtual environment ready."
echo "To activate later:  source .venv/bin/activate"
