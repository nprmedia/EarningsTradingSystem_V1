from __future__ import annotations
import argparse
import sys
import numpy as np
import pandas as pd
from ets.factors.cache_utils import OUT_DIR, update_factors_csv


def robust_z(s: pd.Series) -> pd.Series:
    med = s.median(skipna=True)
    mad = (s - med).abs().median(skipna=True)
    if pd.isna(mad) or mad == 0:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return 0.6745 * (s - med) / mad


def main():
    ap = argparse.ArgumentParser(
        description="Build TREND_raw (smoothed composite) into out/factors_latest.csv"
    )
    ap.add_argument(
        "--symbols",
        default="",
        help="(optional) Path to tickers CSV; otherwise use existing factors file",
    )

    fpath = OUT_DIR / "factors_latest.csv"
    if not fpath.exists():
        print(
            "[FATAL] out/factors_latest.csv missing. Run factor builders first.",
            file=sys.stderr,
        )
        sys.exit(2)
    df = pd.read_csv(fpath)
    if "symbol" not in df.columns:
        print("[FATAL] 'symbol' column missing in factors_latest.csv", file=sys.stderr)
        sys.exit(3)
    df["symbol"] = df["symbol"].astype(str).str.upper()

    for col in ["M_raw", "A_raw", "V_raw"]:
        if col not in df.columns:
            df[col] = 0.0

    # Build V_confirm
    signM = np.sign(df["M_raw"].fillna(0.0))
    Vpos = np.clip(df["V_raw"].fillna(0.0), 0.0, 1.0)
    Vc = signM * Vpos

    zM = robust_z(df["M_raw"].fillna(0.0))
    zA = robust_z(df["A_raw"].fillna(0.0))
    zV = robust_z(Vc)

    trend = 0.5 * zM + 0.3 * zA + 0.2 * zV
    vals = dict(zip(df["symbol"], trend.astype(float).tolist()))

    update_factors_csv(df["symbol"].tolist(), "TREND_raw", vals)
    print("[DONE] TREND_raw complete")


if __name__ == "__main__":
    main()
