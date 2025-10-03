#!/usr/bin/env python3
import os, sys
from datetime import datetime
import pandas as pd
from ets.data.providers.quotes_agg import fetch_quote_basic

def next_day_outcomes(trades_csv: str, asof_date: str):
    tdf = pd.read_csv(trades_csv)
    tickers = tdf["ticker"].astype(str).tolist()
    rows = []
    for t in tickers:
        q = fetch_quote_basic(t)
        if not q: continue
        o = float(q.get("open") or 0.0)
        c = float(q.get("last") or 0.0)
        oc = (c / o - 1.0) if o > 0 else 0.0
        rows.append({
            "run_id": f"{asof_date.replace('-','')}_amc",
            "asof_date": asof_date,
            "ticker": t,
            "next_open_to_close_ret": oc,
            "label_win": 1 if oc > 0.001 else 0,
        })
    if not rows:
        print("No outcomes to write."); return
    os.makedirs("out", exist_ok=True)
    path = os.path.join("out", f"{asof_date.replace('-','')}_amc_outcomes.csv")
    pd.DataFrame(rows).to_csv(path, index=False)
    print(f"[OK] Wrote {path}")

def main():
    if len(sys.argv) < 3:
        print("Usage: ets-outcomes <asof_date YYYY-MM-DD> <trades_csv_path>")
        raise SystemExit(1)
    asof_date, trades_path = sys.argv[1], sys.argv[2]
    next_day_outcomes(trades_path, asof_date)

if __name__ == "__main__":
    main()
