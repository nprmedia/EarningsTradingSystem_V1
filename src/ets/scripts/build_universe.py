import os
import csv
from datetime import datetime, timedelta, UTC
from ets.core.env import load_env
from ets.core.utils import load_yaml, ensure_dirs
from ets.scripts.prefetch_daily import main as prefetch_main
from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.quotes_agg import set_registry, fetch_quote_basic


def _ds(d):
    return d.strftime("%Y-%m-%d")


def _read_calendar(path):
    out = []
    if os.path.exists(path):
        import csv as _csv

        with open(path, "r", encoding="utf-8") as f:
            for r in _csv.DictReader(f):
                out.append(
                    {
                        "date": r.get("date", ""),
                        "symbol": r.get("symbol", "").upper(),
                        "session": r.get("session", "").lower(),
                    }
                )
    return out


def _read_sectors(path):
    m = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for r in csv.reader(f):
                if len(r) >= 2:
                    m[r[0].upper()] = r[1]
    return m


def build_universe():
    load_env()
    cfg = load_yaml(
        os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    )
    u = cfg.get("universe", {})
    horizon = int(u.get("horizon_days", 2))
    session = str(u.get("session", "amc")).lower().strip()
    min_price = float(u.get("min_price", 5.0))
    min_dv = float(u.get("min_dollar_vol", 5e6))
    max_names = int(u.get("max_names", 60))
    allow = [s.strip() for s in (u.get("sector_allowlist") or [])]

    cache_dir = cfg["app"]["cache_dir"]
    ensure_dirs(cache_dir)
    cal_path = os.path.join(cache_dir, "calendar.csv")
    sec_path = os.path.join(cache_dir, "sectors.csv")

    # make sure calendar for next 'horizon' days exists
    base = datetime.now(UTC).date()
    prefetch_main(days=horizon, start=_ds(base))
    cal = _read_calendar(cal_path)

    # filter to window + session
    window = {_ds(base + timedelta(days=i)) for i in range(horizon)}
    symbols = {
        r["symbol"]
        for r in cal
        if r["date"] in window and (session == "" or r["session"] == session)
    }

    # optional sector allowlist
    secmap = _read_sectors(sec_path)
    if allow:
        symbols = {s for s in symbols if secmap.get(s, "Unknown") in allow}

    # liquidity screen
    reg = ProviderRegistry(cfg)
    set_registry(reg)
    keep = []
    for s in sorted(symbols):
        try:
            q = fetch_quote_basic(s) or {}
            last = float(q.get("last") or q.get("close") or 0.0)
            vol = float(q.get("volume") or 0.0)
            if last >= min_price and last * vol >= min_dv:
                keep.append(s)
        except Exception:
            pass

    if max_names > 0 and len(keep) > max_names:
        keep = keep[:max_names]

    with open("tickers.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol"])
        w.writerows([[s] for s in keep])
    print(f"[OK] Rebuilt tickers.csv with {len(keep)} symbols.")
    return keep


if __name__ == "__main__":
    build_universe()
