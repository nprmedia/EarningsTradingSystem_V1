from typing import Dict
import pandas as pd
from ets.core.utils import clip, median

def apply_sector_weights(row: pd.Series, base: Dict, mult: Dict, caps: Dict) -> Dict:
    sec = row.get("sector", "Unknown")
    m = mult.get(sec, mult.get("_default", {"m":1,"v":1,"s":1,"a":1,"tau":1}))
    wm = clip(base["wm"] * m["m"], caps["exp_min"], caps["exp_max"])
    wv = clip(base["wv"] * m["v"], caps["exp_min"], caps["exp_max"])
    ws = clip(base["ws"] * m["s"], caps["exp_min"], caps["exp_max"])
    wa = clip(base["wa"] * m["a"], caps["exp_min"], caps["exp_max"])
    w_sigma = clip(base["w_sigma"], caps["exp_min"], caps["exp_max"])
    w_tau   = clip(base["w_tau"]   * m["tau"], caps["exp_min"], caps["exp_max"])
    return {"wm": wm, "wv": wv, "ws": ws, "wa": wa, "w_sigma": w_sigma, "w_tau": w_tau}

def compute_scores(df: pd.DataFrame, base: Dict, mult: Dict, caps: Dict):
    rows = []
    raw_scores = []
    for _, r in df.iterrows():
        w = apply_sector_weights(r, base, mult, caps)
        # goodness forms: use *_norm for inputs; convert sigma/tau to goodness (1 - norm)
        M = r["M_norm"]; V = r["V_norm"]; S = r["S_norm"]; A = r["A_norm"]
        sigma_g = 1.0 - r["sigma_norm"]
        tau_g   = 1.0 - r["tau_norm"]

        raw = (M ** w["wm"]) * (V ** w["wv"]) * (S ** w["ws"]) * (A ** w["wa"]) * (sigma_g ** w["w_sigma"]) * (tau_g ** w["w_tau"])
        raw_scores.append(raw)
        rows.append({
            **r.to_dict(),
            **w,
            "sigma_g": sigma_g,
            "tau_g": tau_g,
            "raw_score": raw
        })
    tmp = pd.DataFrame(rows)
    C = median(raw_scores) or 1.0
    tmp["score"] = tmp["raw_score"] / C
    tmp.sort_values("score", ascending=False, inplace=True)
    tmp["rank"] = range(1, len(tmp) + 1)
    return tmp
