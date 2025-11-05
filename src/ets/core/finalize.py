import math
from pathlib import Path

import pandas as pd

_RAWS = [
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


def _find_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None


def _safe_nonzero_count(row):
    cnt = 0
    for k in _RAWS:
        if k in row.index:
            val = row[k]
            try:
                x = float(val)
            except Exception:
                x = float("nan")
            if not math.isnan(x) and abs(x) > 1e-12:
                cnt += 1
    return cnt


def finalize_results(scores_path: str, out_dir: str):
    # read scores
    df_s = pd.read_csv(scores_path)
    sym_col = _find_col(df_s, ["symbol", "ticker", "Symbol", "Ticker"])
    score_col = _find_col(df_s, ["score", "Score"])
    if sym_col is None or score_col is None:
        print(
            f"[WARN] finalize: missing symbol/score in {scores_path} (cols: {list(df_s.columns)})"
            " (have cols: {list(df_s.columns)})"
        )
        return

    # derive factors path and read factors for data coverage count
    factors_path = str(Path(scores_path).as_posix()).replace("_scores.csv", "_factors.csv")
    df_f = None
    try:
        df_f = pd.read_csv(factors_path)
        # normalize key column name to 'symbol' for merge
        f_sym = _find_col(df_f, ["symbol", "ticker", "Symbol", "Ticker"]) or "ticker"
        if f_sym != "symbol":
            df_f = df_f.rename(columns={f_sym: "symbol"})
    except Exception:
        pass  # if missing, we’ll default counts to 0

    # Normalize score to 0–100
    s = df_s[score_col].astype(float)
    rng = (s.max() - s.min()) or 1e-9
    final = 100 * (s - s.min()) / rng

    # Labels (placeholder; you’ll calibrate later)
    labels = pd.cut(final, bins=[-1, 49, 69, 100], labels=["SKIP", "HOLD", "BUY"]).astype(str)

    out = pd.DataFrame(
        {
            "symbol": df_s[sym_col].astype(str),
            "final_score": final.round(3),
            "recommendation": labels,
            "confidence": final.round(2),
        }
    )

    # merge per-symbol data_vars (count of non-zero raws)
    if df_f is not None:
        # ensure expected raws exist; if not, missing ones contribute 0
        for col in _RAWS:
            if col not in df_f.columns:
                df_f[col] = 0.0
        # compute count
        df_f["data_vars"] = df_f.apply(_safe_nonzero_count, axis=1)
        out = out.merge(df_f[["symbol", "data_vars"]], on="symbol", how="left")
    else:
        out["data_vars"] = 0

    out_path = Path(out_dir) / Path(scores_path).name.replace("_scores", "_final")
    out.to_csv(out_path, index=False)
    print(f"[OK] Wrote clean output → {out_path}")
