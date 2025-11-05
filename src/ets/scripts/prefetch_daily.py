import contextlib
import csv
import os
import time
from datetime import UTC, datetime, timedelta

from ets.core.env import load_env, require_env
from ets.core.utils import ensure_dirs, load_yaml
from ets.data.providers.finnhub_client import earnings_calendar, profile2
from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.quotes_agg import fetch_quote_basic, set_registry


def _ds(d):
    return d.strftime("%Y-%m-%d")


def _retry(call, *args, tries=5, base_sleep=1.5, **kwargs):
    for i in range(tries):
        try:
            return call(*args, **kwargs)
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(base_sleep * (2**i))


def main(days: int = 7, start: str | None = None):  # noqa: C901
    load_env()
    # Key is strongly recommended; we only warn if missing
    try:
        require_env(
            "FINNHUB_API_KEY",
            "FINNHUB_API_KEY missing. Put it in .env (FINNHUB_API_KEY=...)",
        )
    except Exception as e:
        print("[WARN]", e)

    cfg = load_yaml(os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml"))
    cache_dir = cfg["app"]["cache_dir"]
    ensure_dirs(cache_dir)
    reg = ProviderRegistry(cfg)
    set_registry(reg)

    base = datetime.strptime(start, "%Y-%m-%d").date() if start else datetime.now(UTC).date()
    dates = [_ds(base + timedelta(days=i)) for i in range(days)]

    # 1) calendar
    cal_path = os.path.join(cache_dir, "calendar.csv")
    rows = []
    for ds in dates:
        obj = _retry(earnings_calendar, reg.finnhub, ds, ds) or {}
        for e in obj.get("earningsCalendar", []) or []:
            sym = str(e.get("symbol", "")).upper()
            when = str(e.get("hour", "") or e.get("time", "") or "").lower()
            sess = "amc" if "amc" in when else ("bmo" if "bmo" in when else "")
            rows.append([ds, sym, sess])
    with open(cal_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "symbol", "session"])
        w.writerows(rows)

    # 2) sectors
    sec_path = os.path.join(cache_dir, "sectors.csv")
    existing = {}
    if os.path.exists(sec_path):
        with open(sec_path, encoding="utf-8") as f:
            for r in csv.reader(f):
                if len(r) >= 2:
                    existing[r[0].upper()] = r[1]
    new_secs = {}
    for _, sym, _ in rows:
        if sym and sym not in existing and sym not in new_secs:
            prof = _retry(profile2, reg.finnhub, sym) or {}
            sec = prof.get("finnhubIndustry") or prof.get("sector") or "Unknown"
            new_secs[sym] = sec
    existing.update(new_secs)
    with open(sec_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for k, v in sorted(existing.items()):
            w.writerow([k, v])

    # 3) warm ETF quotes
    for etf in cfg.get("features", {}).get(
        "sector_etfs",
        ["SPY", "XLK", "XLY", "XLF", "XLI", "XLE", "XLV", "XLU", "XLB", "XLRE", "XLC"],
    ):
        with contextlib.suppress(Exception):
            fetch_quote_basic(etf)

    # 4) expire trends cache older than 7 days
    tdir = os.path.join("cache", "trends")
    if os.path.isdir(tdir):
        cutoff = time.time() - 7 * 86400
        for fn in os.listdir(tdir):
            p = os.path.join(tdir, fn)
            try:
                if os.path.getmtime(p) < cutoff:
                    os.remove(p)
            except Exception:
                pass

    print(f"[OK] Prefetch complete: calendar={len(rows)} sectors_added={len(new_secs)}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD (inclusive)")
    args = ap.parse_args()
    main(days=args.days, start=args.start)
