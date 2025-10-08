import argparse
import os
import csv
from datetime import datetime, timedelta
from ets.core.utils import load_yaml, ensure_dirs
from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.quotes_agg import set_registry, fetch_quote_basic
from ets.data.providers.finnhub_client import earnings_calendar, profile2


def _ds(d):
    return d.strftime("%Y-%m-%d")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()

    cfg = load_yaml(
        os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    )
    cache_dir = cfg["app"]["cache_dir"]
    ensure_dirs(cache_dir)

    reg = ProviderRegistry(cfg)
    set_registry(reg)

    # 1) earnings calendars
    today = datetime.utcnow().date()
    dates = [_ds(today + timedelta(days=i)) for i in range(args.days)]
    cal_rows = []
    for ds in dates:
        obj = earnings_calendar(reg.finnhub, ds, ds) or {}
        for e in obj.get("earningsCalendar", []) or []:
            sym = str(e.get("symbol", "")).upper()
            when = str(e.get("hour", "") or e.get("time", "") or "").lower()
            session = "amc" if "amc" in when else ("bmo" if "bmo" in when else "")
            cal_rows.append([ds, sym, session])
    os.makedirs(cache_dir, exist_ok=True)
    with open(
        os.path.join(cache_dir, "calendar.csv"), "w", newline="", encoding="utf-8"
    ) as f:
        w = csv.writer(f)
        w.writerow(["date", "symbol", "session"])
        w.writerows(cal_rows)

    # 2) sector cache via profile2
    sec_path = os.path.join(cache_dir, "sectors.csv")
    existing = {}
    if os.path.exists(sec_path):
        with open(sec_path, "r", encoding="utf-8") as f:
            for r in csv.reader(f):
                if len(r) >= 2:
                    existing[r[0].upper()] = r[1]
    new_secs = {}
    for _, sym, _ in cal_rows:
        if sym and sym not in existing and sym not in new_secs:
            prof = profile2(reg.finnhub, sym) or {}
            sec = prof.get("finnhubIndustry") or prof.get("sector") or "Unknown"
            new_secs[sym] = sec
    existing.update(new_secs)
    with open(sec_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for k, v in sorted(existing.items()):
            w.writerow([k, v])

    # 3) warm sector ETF quotes (improves first-hit latency)
    for etf in cfg.get("features", {}).get(
        "sector_etfs",
        ["SPY", "XLK", "XLY", "XLF", "XLI", "XLE", "XLV", "XLU", "XLB", "XLRE", "XLC"],
    ):
        _ = fetch_quote_basic(etf)

    print(
        f"[OK] Prefetched: calendar={len(cal_rows)} sectors_added={len(new_secs)} etf_warmup={len(cfg.get('features', {}).get('sector_etfs', []))}"
    )


if __name__ == "__main__":
    main()
