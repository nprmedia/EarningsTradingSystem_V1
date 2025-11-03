import os
import sys
import subprocess
import pathlib
import csv

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def test_cli_mock_end_to_end():
    env = os.environ.copy()
    env["ETS_SESSION"] = "amc"
    # Offline run through our wrapper which uses REAL scorer and fixtures
    subprocess.check_call([PYTHON, "scripts/cli_mock_run.py"], env=env, cwd=ROOT)
    # Validate output exists and has schema
    import datetime as dt

    tag = dt.datetime.utcnow().strftime("%Y%m%d")
    f = ROOT / "out" / f"{tag}_amc.csv"
    assert f.exists()
    rows = list(csv.DictReader(open(f)))
    assert rows and set(rows[0].keys()) >= {
        "ticker",
        "sector",
        "price",
        "score",
        "rank",
        "recommendation",
    }
    # Validate log emitted
    log = ROOT / "logs" / f"{tag}_amc.log"
    assert log.exists() and log.stat().st_size > 0
