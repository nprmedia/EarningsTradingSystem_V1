import pandas as pd

def apply_filters_and_select(df_scores: pd.DataFrame,
                             min_price: float,
                             dollar_volume_floor: float,
                             risk_floor_sigma_g: float,
                             risk_floor_tau_g: float,
                             score_threshold: float,
                             max_names: int,
                             max_per_sector: int) -> pd.DataFrame:
    # basic filters
    ok = (df_scores["price"] >= min_price) & \
         (df_scores["dollar_vol"] >= dollar_volume_floor) & \
         (df_scores["sigma_g"] >= risk_floor_sigma_g) & \
         (df_scores["tau_g"] >= risk_floor_tau_g) & \
         (df_scores["score"] >= score_threshold)
    df = df_scores[ok].copy()

    # sector quotas
    picks = []
    per_sector_count = {}
    for _, row in df.sort_values("score", ascending=False).iterrows():
        s = row["sector"]
        cnt = per_sector_count.get(s, 0)
        if cnt < max_per_sector:
            picks.append(row)
            per_sector_count[s] = cnt + 1
        if len(picks) >= max_names:
            break

    out = pd.DataFrame(picks)
    out["passed_filters"] = 1
    return out
