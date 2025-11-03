#!/usr/bin/env python3
"""Generate analytics summaries with optional system metrics."""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any, Dict


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _prepare_environment() -> Path:
    root = _repo_root()
    sys.path.insert(0, str(root / "src"))
    os.chdir(root)
    (root / "metrics").mkdir(parents=True, exist_ok=True)
    (root / "reports").mkdir(parents=True, exist_ok=True)
    return root


def _process_metrics() -> Dict[str, Any]:
    try:
        import psutil  # type: ignore
    except Exception:
        return {"rss_bytes": None, "cpu_percent": None}

    proc = psutil.Process()
    return {
        "rss_bytes": proc.memory_info().rss,
        "cpu_percent": proc.cpu_percent(interval=0.05),
    }


def main() -> int:
    root = _prepare_environment()

    from ets.analysis.history_compare import compare_latest_two
    from ets.analysis.sector_summary import run_summary

    session = os.getenv("ETS_SESSION", "amc")
    start = time.perf_counter()

    tracemalloc.start()
    summary = run_summary(session=session)
    try:
        history = compare_latest_two()
    except FileNotFoundError:
        history = {"note": "only one output present; run pipeline twice to enable history compare", "count": 0}
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    elapsed = time.perf_counter() - start
    metrics: Dict[str, Any] = {
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        "session": session,
        "elapsed_sec": elapsed,
        "summary_top_n": summary.get("top_n", []),
        "history_count": history.get("count", 0) if isinstance(history, dict) else 0,
        "tracemalloc_current": current,
        "tracemalloc_peak": peak,
    }
    metrics.update(_process_metrics())

    metrics_path = root / "metrics" / "analytics_perf.json"
    metrics_path.write_text(json.dumps(metrics, indent=2))
    print(f"[OK] Analytics complete. metrics={metrics_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
