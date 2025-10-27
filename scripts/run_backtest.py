import sys
import os
import time
import pathlib

# Add src to PYTHONPATH early
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from src.ets.backtest.historical_loader import load_signals, load_history, make_panel
from src.ets.backtest.performance_metrics import (
    compute_metrics,
    save_artifacts,
    save_perf,
)


if __name__ == "__main__":
    # --- normalize paths for CI ---
    ROOT = pathlib.Path(__file__).resolve().parents[1]
    os.chdir(ROOT)

    OUT_DIR = ROOT / "reports"
    MET_DIR = ROOT / "metrics"
    OUT_DIR.mkdir(exist_ok=True)
    MET_DIR.mkdir(exist_ok=True)

    print("[INFO] Running backtest...")
    sig = load_signals(os.getenv("BACKTEST_SIGNALS", "tests/fixtures/mock_signals.csv"))
    hist = load_history(
        os.getenv("BACKTEST_HISTORY", "tests/fixtures/mock_history.csv")
    )
    panel = make_panel(sig, hist)
    metrics = compute_metrics(panel)
    save_artifacts(OUT_DIR, metrics)
    save_perf(MET_DIR, time.perf_counter(), len(panel))
    print("[OK] Backtest complete.")
