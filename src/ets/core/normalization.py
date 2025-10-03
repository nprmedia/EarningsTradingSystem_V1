import numpy as np
import pandas as pd

def _winsorize_series(s: pd.Series, p_low: float, p_high: float) -> pd.Series:
    lo = s.quantile(p_low)
    hi = s.quantile(p_high)
    return s.clip(lower=lo, upper=hi)

def _robust_sigmoid(z: np.ndarray) -> np.ndarray:
    # maps z to (0,1)
    return 1.0 / (1.0 + np.exp(-z))

def robust_normalize_df(df: pd.DataFrame, cols: list, p_low=0.10, p_high=0.90,
                        min_universe_for_robust=40, clip_min=0.05, clip_max=0.95) -> pd.DataFrame:
    """Winsorize -> robust z via MAD -> sigmoid -> clip [clip_min, clip_max] per column."""
    out = df.copy()
    n = len(df)
    for c in cols:
        x = out[c].astype(float)
        if n < min_universe_for_robust:
            # fallback: min-max
            mn, mx = x.min(), x.max()
            rng = (mx - mn) if (mx > mn) else 1.0
            nrm = (x - mn) / rng
        else:
            w = _winsorize_series(x, p_low, p_high)
            med = w.median()
            mad = np.median(np.abs(w - med))
            mad = mad if mad > 1e-9 else 1e-9
            z = 0.6745 * (w - med) / mad
            nrm = pd.Series(_robust_sigmoid(z), index=w.index)
        out[c + "_norm"] = nrm.clip(lower=clip_min, upper=clip_max)
    return out
