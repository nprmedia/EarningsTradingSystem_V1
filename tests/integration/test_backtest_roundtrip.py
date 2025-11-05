from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest


@pytest.mark.integration
def test_backtest_roundtrip(tmp_out: pathlib.Path, fixtures_dir: pathlib.Path):
    env = os.environ.copy()
    env.update({"ETS_OFFLINE": "1", "ETS_SEED": "4242", "ETS_STRICT_DETERMINISM": "1"})
    tickers = fixtures_dir / "tickers_small.csv"
    date, session = "2024-10-02", "amc"

    cmd_pkg = [
        sys.executable,
        "-m",
        "ets.main",
        "--session",
        session,
        "--date",
        date,
        "--tickers",
        str(tickers),
        "--out",
        str(tmp_out),
    ]
    cmd_script = [
        sys.executable,
        "scripts/run_backtest.py",
        "--session",
        session,
        "--date",
        date,
        "--tickers",
        str(tickers),
        "--out",
        str(tmp_out),
    ]

    try:
        subprocess.run(cmd_pkg, check=True, env=env)
    except subprocess.CalledProcessError:
        subprocess.run(cmd_script, check=True, env=env)

    csvs = list(tmp_out.glob("*_trades.csv"))
    reports = list(tmp_out.glob("*.json")) + list(tmp_out.glob("*.html"))
    assert csvs or reports, "Expected at least one artifact in out/"
