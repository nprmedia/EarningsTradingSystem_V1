import os
import sys
import subprocess
import pathlib
import json
import csv

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def test_backtest_engine_core():
    # Run backtest using fixtures
    env = os.environ.copy()
    env["BACKTEST_SIGNALS"] = str(ROOT / "tests/fixtures/mock_signals.csv")
    env["BACKTEST_HISTORY"] = str(ROOT / "tests/fixtures/mock_history.csv")
    subprocess.check_call([PYTHON, "scripts/run_backtest.py"], env=env, cwd=ROOT)

    # Artifacts
    summary = ROOT / "out" / "backtest_summary.json"
    sector_csv = ROOT / "out" / "sector_accuracy.csv"
    assert summary.exists(), "Missing backtest_summary.json"
    assert sector_csv.exists(), "Missing sector_accuracy.csv"

    data = json.loads(summary.read_text())
    assert "hit_rate" in data and data["count"] >= 1
    # sector file has expected columns
    rows = list(csv.DictReader(open(sector_csv)))
    assert rows and set(rows[0].keys()) >= {"date", "ticker", "signal", "ret_next"}
