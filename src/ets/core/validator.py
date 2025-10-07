from __future__ import annotations
import pandas as pd

REQUIRED_RAWS = [
    "M_raw","V_raw","S_raw","A_raw","sigma_raw","tau_raw",
    "CAL_raw","SRM_raw","PEER_raw","ETFF_raw","VIX_raw","TREND_raw",
]

def validate_factors(df: pd.DataFrame, min_rows: int = 1, min_coverage: float = 0.8):
    if df is None or df.empty:
        raise RuntimeError("No factor rows computed.")
    missing_cols = [c for c in REQUIRED_RAWS if c not in df.columns]
    if missing_cols:
        raise RuntimeError(f"Missing factor columns: {missing_cols}")
    # coverage per required col
    good = df[REQUIRED_RAWS].notna().astype(int)
    coverage = float(good.sum().sum()) / float(len(REQUIRED_RAWS) * len(df))
    if len(df) < min_rows:
        raise RuntimeError(f"Too few rows: {len(df)} < {min_rows}")
    if coverage < min_coverage:
        raise RuntimeError(f"Coverage {coverage:.2%} below threshold {min_coverage:.2%}")
    return coverage
