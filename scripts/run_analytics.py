import os
import time
import json
import pathlib
import datetime
import tracemalloc
import psutil
from src.ets.analysis.sector_summary import run_summary
from src.ets.analysis.history_compare import compare_latest_two

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
MET = ROOT / "metrics"
MET.mkdir(parents=True, exist_ok=True)


def main():
    session = os.getenv("ETS_SESSION", "amc")
    start = time.perf_counter()
    tracemalloc.start()
    proc = psutil.Process()

    summary = run_summary(session=session)

    hist = {}
    try:
        hist = compare_latest_two()
    except FileNotFoundError:
        hist = {
            "note": "only one output present; run pipeline twice to enable history compare"
        }

    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    elapsed = time.perf_counter() - start

    # Capture psutil metrics
    metrics = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "session": session,
        "elapsed_sec": elapsed,
        "rss_bytes": proc.memory_info().rss,
        "cpu_percent": proc.cpu_percent(interval=0.05),
        "summary_top_n": summary.get("top_n", []),
        "history_count": hist.get("count", 0) if isinstance(hist, dict) else 0,
        "tracemalloc_current": cur,
        "tracemalloc_peak": peak,
    }
    date_tag = datetime.datetime.utcnow().strftime("%Y%m%d")
    metrics_path = MET / f"{date_tag}_{session}_perf.json"
    metrics_path.write_text(json.dumps(metrics, indent=2))
    print(f"[OK] Analytics complete. metrics={metrics_path}")


if __name__ == "__main__":
    main()
