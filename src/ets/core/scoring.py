from __future__ import annotations
import pandas as pd

# expected normalized column names we’ll combine
_COMBOS = [
    ("m",        "M_norm"),
    ("v",        "V_norm"),
    ("s",        "S_norm"),
    ("a",        "A_norm"),
    ("sigma",    "sigma_norm"),
    ("tau",      "tau_norm"),
    ("cal",      "CAL_norm"),
    ("srm",      "SRM_norm"),
    ("peer",     "PEER_norm"),
    ("etff",     "ETFF_norm"),
    ("vix_risk", "VIX_norm"),
    ("trend",    "TREND_norm"),
]

def _get_col(df: pd.DataFrame, name: str):
    return df[name] if name in df.columns else 0.0

def compute_scores(df: pd.DataFrame, base: dict, mult: dict, caps: dict) -> pd.DataFrame:
    out = df.copy()

    # sector multipliers: per-sector dicts; fallback to _default
    def w_for(sector: str, key: str) -> float:
        sec = mult.get(sector, mult.get("_default", {}))
        return float(sec.get(key, 1.0))

    score = 0.0
    # accumulate weighted sum
    total = None
    for key, col in _COMBOS:
        bw = float(base.get(key, 0.0))
        vec = _get_col(out, col)
        # sector multiplier per row
        sec_mult = out["sector"].map(lambda s: w_for(s, key))
        term = bw * vec * sec_mult
        total = term if total is None else total + term

    out["score"] = total.fillna(0.0)

    # include a few diagnostics
    out["score_base_sum"] = sum(float(base.get(k, 0.0)) for k, _ in _COMBOS)
    return out
