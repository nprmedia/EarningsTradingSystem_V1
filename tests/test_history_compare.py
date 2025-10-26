import os
import sys
import subprocess
import pathlib
import time
import json

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def test_history_compare_latest_two():
    env = os.environ.copy()
    env["ETS_SESSION"] = "amc"
    # Produce two outputs (rotate naming will keep latest)
    subprocess.check_call([PYTHON, "scripts/cli_mock_run.py"], env=env, cwd=ROOT)
    time.sleep(1)
    subprocess.check_call([PYTHON, "scripts/cli_mock_run.py"], env=env, cwd=ROOT)
    # Run analytics (will attempt compare)
    subprocess.check_call([PYTHON, "scripts/run_analytics.py"], env=env, cwd=ROOT)
    hist = json.loads((ROOT / "out" / "history_delta.json").read_text())
    assert (
        "previous" in hist and "current" in hist and isinstance(hist.get("diffs"), list)
    )
