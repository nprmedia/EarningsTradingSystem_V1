from __future__ import annotations

import runpy
import sys
from io import StringIO


def test_cli_help_prints(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["-m", "ets", "--help"])
    buf = StringIO()
    monkeypatch.setattr(sys, "stdout", buf)
    try:
        runpy.run_module("ets", run_name="__main__")
    except SystemExit as e:
        assert e.code == 0
    out = buf.getvalue().lower()
    assert "usage" in out or "help" in out
