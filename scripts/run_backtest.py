import pandas as pd
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
import os
import time
from pathlib import Path

from src.ets.backtest.historical_loader import load_signals, load_history, make_panel
from src.ets.backtest.performance_metrics import (
    compute_metrics,
    save_artifacts,
    save_perf,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
MET = ROOT / "metrics"


def main():
    signals_path = Path(
        os.getenv("BACKTEST_SIGNALS", ROOT / "tests/fixtures/mock_signals.csv")
    )
    history_path = Path(
        os.getenv("BACKTEST_HISTORY", ROOT / "tests/fixtures/mock_history.csv")
    )

    t0 = time.perf_counter()
    signals = load_signals(signals_path)
    history = load_history(history_path)
    panel = make_panel(signals, history)
    metrics = compute_metrics(panel)
    save_artifacts(metrics, OUT, panel)
    save_perf(MET, time.perf_counter() - t0, len(panel))
    print("[OK] Backtest complete.", metrics)
    from src.ets.backtest.perf_metrics import compute_perf

    compute_perf(pd.read_csv(OUT))
    from src.ets.backtest.perf_metrics import compute_perf

    compute_perf(pd.read_csv(OUT))


if __name__ == "__main__":
    main()
