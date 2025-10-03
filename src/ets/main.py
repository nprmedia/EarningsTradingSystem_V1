import os, argparse, sys, logging, time
from datetime import datetime
import pandas as pd

logging.getLogger("yfinance").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

from ets.core.utils import load_yaml, run_id, ensure_dirs
from ets.data.calendar import from_finnhub, from_csv
from ets.data.market_refs import sector_etf_pct, spy_pct, vix_level
from ets.data.providers.quotes_agg import fetch_quote_basic
from ets.core.factors import compute_raw_factors
from ets.core.normalization import robust_normalize_df
from ets.core.scoring import compute_scores
from ets.core.selection import apply_filters_and_select
from ets.outputs.csv_writer import write_factors, write_scores, write_trades

def parse_args():
    ap = argparse.ArgumentParser(description="Pre-earnings MVP (free-tier)")
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--session", default="amc", choices=["amc","bmo"])
    ap.add_argument("--tickers", help="CSV with tickers (one per line). If omitted, tries Finnhub.")
    return ap.parse_args()

def load_configs():
    cfg = load_yaml("src/ets/config/config.yaml")
    wts = load_yaml("src/ets/config/weights.yaml")
    ensure_dirs(cfg["app"]["out_dir"], cfg["app"]["logs_dir"], cfg["app"]["cache_dir"])
    return cfg, wts

def build_universe(date_str: str, tickers_csv: str | None):
    return from_csv(tickers_csv) if tickers_csv else from_finnhub(date_str)

def main():
    args = parse_args()
    cfg, wts = load_configs()
    rid = run_id(args.date, args.session)

    sector_map = cfg["etfs"]["sectors"]
    universe = build_universe(args.date, args.tickers) or []
    if not universe:
        print("No tickers found."); sys.exit(0)

    raw_rows = []
    for idx, t in enumerate(universe):
        row = compute_raw_factors(t, sector_map)
        if row: raw_rows.append(row)
        if idx % 3 == 0: time.sleep(0.35)

    if not raw_rows:
        print("No factor rows computed."); sys.exit(0)

    factors_df = pd.DataFrame(raw_rows)
    norm_cols = ["M_raw","V_raw","S_raw","A_raw","sigma_raw","tau_raw"]
    factors_df = robust_normalize_df(
        factors_df, cols=norm_cols,
        p_low=cfg["normalization"]["winsor_p_low"],
        p_high=cfg["normalization"]["winsor_p_high"],
        min_universe_for_robust=cfg["normalization"]["min_universe_for_robust"],
        clip_min=cfg["normalization"]["clip_min"],
        clip_max=cfg["normalization"]["clip_max"],
    )
    rename_map = {c+"_norm": c.replace("_raw","")+"_norm" for c in norm_cols}
    factors_df.rename(columns=rename_map, inplace=True)

    df_scores = compute_scores(factors_df, base=wts["base"], mult=wts["multipliers"], caps=wts["caps"])

    sel = cfg["selection"]; uni = cfg["universe"]
    df_trades = apply_filters_and_select(
        df_scores,
        min_price=uni["min_price"],
        dollar_volume_floor=uni["dollar_volume_floor"],
        risk_floor_sigma_g=sel["risk_floor_sigma_g"],
        risk_floor_tau_g=sel["risk_floor_tau_g"],
        score_threshold=sel["score_threshold"],
        max_names=sel["max_names"],
        max_per_sector=sel["max_per_sector"],
    )

    out_dir = cfg["app"]["out_dir"]
    factors_path = os.path.join(out_dir, f"{rid}_factors.csv")
    scores_path  = os.path.join(out_dir, f"{rid}_scores.csv")
    trades_path  = os.path.join(out_dir, f"{rid}_trades.csv")

    write_factors(factors_path, raw_rows)
    df_scores.to_csv(scores_path, index=False)
    df_trades.to_csv(trades_path, index=False)

    print(f"[OK] Wrote:\n  {factors_path}\n  {scores_path}\n  {trades_path}")

if __name__ == "__main__":
    main()
