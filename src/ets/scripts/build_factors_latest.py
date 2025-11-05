from __future__ import annotations

from pathlib import Path

import pandas as pd

FACTORS = [
    "M_raw",
    "V_raw",
    "S_raw",
    "A_raw",
    "sigma_raw",
    "tau_raw",
    "CAL_raw",
    "SRM_raw",
    "PEER_raw",
    "ETFF_raw",
    "VIX_raw",
    "TREND_raw",
]


def main():
    out = Path("out")
    # If missing, create a minimal placeholder
    # using symbols from quote_results.csv
    tgt = out / "factors_latest.csv"
    if tgt.exists():
        print(f"[INFO] factors_latest.csv already exists: {tgt}")
        return
    seed = out / "quote_results.csv"
    if not seed.exists():
        raise SystemExit(
            "Seed file not found: out/quote_results.csv. Run quotes or provide your factors CSV."
        )
    df = pd.read_csv(seed)
    base = pd.DataFrame({"symbol": df["symbol"].astype(str).str.upper().dropna().unique()})
    for f in FACTORS:
        base[f] = 0.0
    base.to_csv(tgt, index=False)
    print(
        f"[INFO] wrote placeholder {tgt} with {len(base)} symbols;"
        " fill real factor columns upstream."
    )


if __name__ == "__main__":
    main()
