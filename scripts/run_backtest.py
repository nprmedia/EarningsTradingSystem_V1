#!/usr/bin/env python3
"""
Backtest runner (CI-safe, Ruff/Black-compliant, hardened)

- No project imports at module scope (avoids Ruff E402)
- Resolves repo root robustly across local/CI
- Adds src/ to sys.path inside main() before project imports
- Creates reports/ and metrics/ if missing; checks writability
- Validates inputs early; clear diagnostics
- Optional flags: --verbose, --strict, --dry-run, --seed
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional, Sequence


def _repo_root_from_git(start: Path) -> Optional[Path]:
    """Attempt to find the git toplevel (best-effort; returns None if not a git repo)."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(start),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        p = Path(out)
        return p if (p / "src").is_dir() else None
    except Exception:
        return None


def _resolve_root() -> Path:
    """
    Find the repository root in local dev and CI, robustly.

    Search order:
      1) git toplevel (if available) that contains src/
      2) nearest ancestor of this file containing src/
      3) fallback: script directory
    """
    here = Path(__file__).resolve()

    root = _repo_root_from_git(here)
    if root:
        return root

    for parent in [here, *here.parents]:
        candidate = parent if parent.is_dir() else parent.parent
        if (candidate / "src").is_dir():
            return candidate

    # Last resort: script directory
    return here.parent


def _ensure_dirs_writable(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)
        test_file = p / ".write_test"
        try:
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
        except Exception as e:  # pragma: no cover
            raise PermissionError(f"Directory not writable: {p}") from e


def _expand(p: str | Path) -> Path:
    return Path(str(p)).expanduser().resolve()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run ETS backtest with fixtures")
    ap.add_argument(
        "--signals",
        default=os.getenv("BACKTEST_SIGNALS", "tests/fixtures/mock_signals.csv"),
        help="Path to signals CSV (env BACKTEST_SIGNALS overrides)",
    )
    ap.add_argument(
        "--history",
        default=os.getenv("BACKTEST_HISTORY", "tests/fixtures/mock_history.csv"),
        help="Path to historical prices CSV (env BACKTEST_HISTORY overrides)",
    )
    ap.add_argument(
        "--reports",
        default=os.getenv("BACKTEST_REPORTS", "reports"),
        help="Output directory for human-readable reports",
    )
    ap.add_argument(
        "--metrics",
        default=os.getenv("BACKTEST_METRICS", "metrics"),
        help="Output directory for machine-readable metrics",
    )
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging (INFO). Use twice (-vv) for DEBUG via env LOGLEVEL=DEBUG.",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Fail on common data issues (e.g., empty panel) with non-zero exit code.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Load inputs and build panel, but skip writing outputs.",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional randomness seed (forward-compatible; echoed for determinism).",
    )
    return ap.parse_args(argv)


def _configure_logging(verbose: bool) -> None:
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def _preflight_inputs(signals: Path, history: Path) -> None:
    missing: list[str] = []
    if not signals.is_file():
        missing.append(f"signals: {signals}")
    if not history.is_file():
        missing.append(f"history: {history}")
    if missing:
        raise FileNotFoundError("Input file(s) not found:\n  " + "\n  ".join(missing))
    # Optional: quick size sanity
    for p in (signals, history):
        if p.stat().st_size == 0:
            raise ValueError(f"Input appears empty: {p}")


def _write_last_run(
    metrics_dir: Path, status: str, duration_s: float, extra: dict
) -> None:
    payload = {
        "status": status,
        "duration_seconds": duration_s,
        "python": sys.version.split()[0],
        "platform": sys.platform,
        **extra,
    }
    out = metrics_dir / "_last_run.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    t0 = time.perf_counter()

    try:
        ROOT = _resolve_root()
        os.chdir(ROOT)

        args = parse_args(argv)
        _configure_logging(args.verbose)
        log = logging.getLogger("backtest")

        reports = _expand(args.reports)
        metrics = _expand(args.metrics)
        signals = _expand(args.signals)
        history = _expand(args.history)

        print("[INFO] Backtest starting")
        print(f"  ROOT      = {ROOT}")
        print(f"  signals   = {signals}")
        print(f"  history   = {history}")
        print(f"  reports   = {reports}")
        print(f"  metrics   = {metrics}")
        if args.seed is not None:
            print(f"  seed      = {args.seed}")

        # Preflight I/O
        _ensure_dirs_writable(reports, metrics)
        _preflight_inputs(signals, history)

        # Make src/ importable before project imports (avoid Ruff E402)
        sys.path.insert(0, str(ROOT / "src"))

        # Import project modules inside main
        try:
            from src.ets.backtest.historical_loader import (  # type: ignore
                load_signals,
                load_history,
                make_panel,
            )
            from src.ets.backtest.performance_metrics import (  # type: ignore
                compute_metrics,
                save_artifacts,
                save_perf,
            )
        except ModuleNotFoundError:
            print("\n[ERROR] Could not import 'src' packages.")
            print(f"  cwd: {Path.cwd()}")
            print(f"  expected src path: {ROOT / 'src'}")
            print(f"  sys.path[0]: {sys.path[0]}")
            print(f"  sys.path: {json.dumps(sys.path[:8], indent=2)}")
            print(
                "  Hint: Ensure that 'src/' exists at the repo root and contains __init__.py in 'src' and 'src/ets'."
            )
            raise

        # Load fixtures and build panel
        sig = load_signals(str(signals))
        hist = load_history(str(history))
        panel = make_panel(sig, hist)

        # Basic sanity before compute
        try:
            n = len(panel)  # many panels implement __len__
        except Exception:
            n = -1  # unknown

        if args.strict:
            if n == 0:
                raise ValueError("Strict mode: empty panel after make_panel().")
            if n < 0:
                raise ValueError(
                    "Strict mode: panel length unknown; implement __len__ or add a size check."
                )

        log.info("Panel size: %s", n if n >= 0 else "unknown")

        duration_build = time.perf_counter() - t0

        if args.dry_run:
            print("[OK] Dry-run complete (skipped metrics/artifacts).")
            _write_last_run(
                metrics,
                status="dry-run",
                duration_s=duration_build,
                extra={
                    "signals": str(signals),
                    "history": str(history),
                    "panel_length": n,
                },
            )
            return 0

        # Compute + persist
        m = compute_metrics(panel)
        save_artifacts(reports, m)  # should write summary CSV/JSON
        total_s = time.perf_counter() - t0
        save_perf(metrics, total_s, n)

        _write_last_run(
            metrics,
            status="ok",
            duration_s=total_s,
            extra={
                "signals": str(signals),
                "history": str(history),
                "panel_length": n,
                "build_seconds": duration_build,
            },
        )

        print("[OK] Backtest complete.")
        return 0

    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Backtest aborted by user.")
        return 130  # conventional SIGINT code
    except Exception as e:
        total_s = time.perf_counter() - t0
        # Best effort to write failure marker if metrics dir exists or can be created
        try:
            metrics_dir = _expand(os.getenv("BACKTEST_METRICS", "metrics"))
            metrics_dir.mkdir(parents=True, exist_ok=True)
            _write_last_run(
                metrics_dir,
                status="error",
                duration_s=total_s,
                extra={"error": repr(e)},
            )
        except Exception:
            pass
        print(f"[FAIL] {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
