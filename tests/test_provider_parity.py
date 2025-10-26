import os
import sys
import subprocess
import pathlib
import json

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def test_provider_validation_mock_mode():
    env = os.environ.copy()
    env["ETS_API_MODE"] = "mock"
    env["ETS_SYMBOLS"] = "AAPL,MSFT,XOM"
    # run validator
    subprocess.check_call([PYTHON, "scripts/validate_providers.py"], env=env, cwd=ROOT)
    # artifacts exist
    schema = ROOT / "out" / "provider_schema_diff.json"
    vrange = ROOT / "out" / "provider_range_report.json"
    perf = ROOT / "metrics" / "provider_perf.json"
    assert schema.exists() and vrange.exists() and perf.exists()
    data = json.loads(schema.read_text())
    assert "schemas" in data and "diff" in data
    # yahoo should be present in schemas; mock must be present too
    keys = set(data["schemas"].keys())
    assert {"mock", "yahoo"}.issubset(keys)
