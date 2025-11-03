from __future__ import annotations
import pandas as pd
from pathlib import Path


def rebuild_signals_from_fixture(path: Path) -> pd.DataFrame:
    # In Part A we simply read the provided fixture.
    return pd.read_csv(path, parse_dates=["date"])
