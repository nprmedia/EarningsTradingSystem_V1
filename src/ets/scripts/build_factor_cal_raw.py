from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import sys
from pathlib import Path

from ets.data.providers.provider_registry import ProviderRegistry
from ets.factors.cache_utils import read_symbols, update_factors_csv

try:
    import requests
except Exception:
    print("[FATAL] requests required", file=sys.stderr)
    raise

CACHE_FILE = Path("out/cache/calendar.csv")


def _load_cfg():
    # same search order as other scripts
    from pathlib import Path

    here = Path(__file__).resolve()
    for rel in [
        "src/ets/config/config.yaml",
        "ets/config/config.yaml",
        "config/config.yaml",
    ]:
        p = here.parents[2] / rel
        if p.exists():
            import yaml

            return yaml.safe_load(p.read_text())
    raise FileNotFoundError("config.yaml not found")


def _load_calendar_cache() -> dict[tuple[str, str], str]:
    # key: (symbol, "next"), value: YYYY-MM-DD
    out: dict[tuple[str, str], str] = {}
    if not CACHE_FILE.exists():
        return out
    with CACHE_FILE.open() as f:
        r = csv.DictReader(f)
        for row in r:
            out[(row["symbol"].upper(), row["kind"])] = row["date"]
    return out


def _save_calendar_cache(rows: list[dict[str, str]]) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    # de-dup by (symbol,kind)
    dedup: dict[tuple[str, str], dict[str, str]] = {}
    if CACHE_FILE.exists():
        with CACHE_FILE.open() as f:
            for row in csv.DictReader(f):
                dedup[(row["symbol"].upper(), row["kind"])] = row
    for r in rows:
        dedup[(r["symbol"].upper(), r["kind"])] = r
    with CACHE_FILE.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "kind", "date", "checked_at"])
        w.writeheader()
        for r in dedup.values():
            w.writerow(r)


def _days_until(d: str) -> int:
    # d: "YYYY-MM-DD"
    today = dt.date.today()
    try:
        target = dt.datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return 9999
    return max(0, (target - today).days)


def _fetch_next_earnings(reg: dict, symbol: str, horizon_days: int = 60) -> str | None:
    """Call Finnhub calendar within [today, today+horizon], return earliest date for symbol."""
    base = reg.get("base")
    key = reg.get("key")
    if not key:
        return None
    today = dt.date.today()
    to = today + dt.timedelta(days=horizon_days)
    url = f"{base}/calendar/earnings"
    params = {
        "from": today.isoformat(),
        "to": to.isoformat(),
        "symbol": symbol,
        "token": key,
    }
    sess = reg.get("session")
    if reg.get("limiter"):
        reg["limiter"].acquire(cost=1)
    try:
        r = (sess or requests).get(url, params=params, timeout=8)
        if r.status_code != 200:
            return None
        js = r.json() or {}
        eps = js.get("earningsCalendar") or []
        dates = sorted(
            [
                row.get("date")
                for row in eps
                if (row.get("symbol") or "").upper() == symbol and row.get("date")
            ]
        )
        return dates[0] if dates else None
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(
        description="Build CAL_raw (earnings proximity) into out/factors_latest.csv"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--horizon", type=int, default=60, help="days ahead to search")
    ap.add_argument(
        "--decay_k", type=int, default=7, help="decay constant in days for exp(-days/k)"
    )
    args = ap.parse_args()

    symbols: list[str] = read_symbols(args.symbols)
    cfg = _load_cfg()
    reg = ProviderRegistry(cfg).finnhub  # uses your .env key & limiter
    cache = _load_calendar_cache()

    updates_cache: list[dict[str, str]] = []
    cal_vals: dict[str, float] = {}
    for s in symbols:
        sU = s.upper()
        cached = cache.get((sU, "next"))
        if cached is None:
            nxt = _fetch_next_earnings(reg, sU, horizon_days=args.horizon)
            if nxt:
                updates_cache.append(
                    {
                        "symbol": sU,
                        "kind": "next",
                        "date": nxt,
                        "checked_at": dt.datetime.utcnow().isoformat() + "Z",
                    }
                )
                d = _days_until(nxt)
                cal_vals[sU] = float(math.exp(-max(0, d) / max(1, args.decay_k)))
            else:
                cal_vals[sU] = 0.0
        else:
            d = _days_until(cached)
            # Optional: refresh cache if stale (>24h) and event not passed
            if (
                d > 0 and dt.datetime.fromisoformat(updates_cache[-1]["checked_at"][:-1]).date()
                if updates_cache
                else True
            ):
                # keep cached for now; lightweight
                pass
            cal_vals[sU] = float(math.exp(-max(0, d) / max(1, args.decay_k)))

    if updates_cache:
        _save_calendar_cache(updates_cache)

    update_factors_csv(symbols, "CAL_raw", cal_vals)
    print("[DONE] CAL_raw complete")


if __name__ == "__main__":
    main()
