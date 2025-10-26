import os
import sys
import subprocess
import pathlib
import json
import datetime as dt

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def _today_csv(session="amc"):
    return ROOT / "out" / f"{dt.datetime.utcnow().strftime('%Y%m%d')}_{session}.csv"


def test_sector_summary_and_topn():
    env = os.environ.copy()
    env["ETS_SESSION"] = "amc"
    # Ensure an output exists
    subprocess.check_call([PYTHON, "scripts/cli_mock_run.py"], env=env, cwd=ROOT)
    # Run analytics summary
    subprocess.check_call([PYTHON, "scripts/run_analytics.py"], env=env, cwd=ROOT)
    # Check artifacts
    ss_json = ROOT / "out" / "sector_summary.json"
    top_json = ROOT / "out" / "top5.json"
    assert ss_json.exists() and ss_json.stat().st_size > 0
    assert top_json.exists() and top_json.stat().st_size > 0
    top = json.loads(top_json.read_text())
    assert isinstance(top, list) and len(top) >= 1
