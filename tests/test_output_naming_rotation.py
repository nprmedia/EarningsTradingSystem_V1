import os
import pathlib
import subprocess
import sys
import time
import datetime as dt

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def _today_name(session="amc"):
    tag = dt.datetime.utcnow().strftime("%Y%m%d")
    return ROOT / "out" / f"{tag}_{session}.csv"


def test_naming_and_rotation():
    # First run
    env = os.environ.copy()
    env["ETS_SESSION"] = "amc"
    subprocess.check_call([PYTHON, "scripts/cli_mock_run.py"], env=env, cwd=ROOT)
    f1 = _today_name("amc")
    assert f1.exists()
    size1 = f1.stat().st_size
    time.sleep(1)
    # Second run triggers rotation
    subprocess.check_call([PYTHON, "scripts/cli_mock_run.py"], env=env, cwd=ROOT)
    f2 = _today_name("amc")
    assert f2.exists()
    size2 = f2.stat().st_size
    assert size2 == size1  # deterministic content on same inputs
