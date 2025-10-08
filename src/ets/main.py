import os
import sys
import argparse
import logging
import time
import csv
from datetime import datetime

import pandas as pd

# --- Project / internal imports
from ets.config.defaults import DEFAULT_WEIGHTS
from ets.util.dicts import deep_merge
from ets.core.utils import load_yaml, run_id, ensure_dirs
from ets.data.calendar import from_finnhub, from_csv
from ets.core.factors import compute_raw_factors
from ets.core.normalization import robust_normalize_df
from ets.core.scoring import compute_scores
from ets.core.selection import apply_filters_and_select
from ets.outputs.csv_writer import write_factors, write_scores, write_trades, write_pulls
from ets.outputs.telemetry import write_telemetry
from ets.core.env import load_env, require_env
from ets.data.signals.calendar_loader import set_fallback_peers
from ets.core.finalize import finalize_results
from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.quotes_agg import set_registry, get_pull_log
from ets.data.signals.calendar_loader import set_registry as set_calendar_registry

# Quiet noisy libs
logging.getLogger("yfinance").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

# -------- Defaults to avoid KeyError on cfg[...] ----------
DEFAULT_CFG = {
    "app": {"out_dir": "out", "logs_dir": "logs", "cache_dir": "cache"},
    "normalization": {
        "winsor_p_low": 0.01,
        "winsor_p_high": 0.99,
        "min_universe_for_robust": 5,
        "clip_min": -3.0,
        "clip_max": 3.0,
    },
    "selection": {
        "risk_floor_sigma_g": -1e9,
        "risk_floor_tau_g": -1e9,
        "score_threshold": -1e9,
        "max_names": 10,
        "max_per_sector": 3,
    },
    "universe": {
        "min_price": 0.0,
        "dollar_volume_floor": 0.0,
    },
}



def _coerce_base(base_val, norm_cols):
    """
    Ensure base is a dict keyed by factor names (e.g., 'M','V','S',...).
    If base_val is scalar, broadcast to all factor keys inferred from norm_cols.
    norm_cols are like ["M_raw","V_raw",...]; we strip "_raw" to get keys.
    """
    # infer factor keys from norm_cols: "M_raw" -> "M"
    factor_keys = [c.replace("_raw", "") for c in (norm_cols or [])]
    if not factor_keys:
        factor_keys = ["M","V","S","A","sigma","tau","CAL","SRM","PEER","ETFF","VIX","TREND"]
    # dict already
    if isinstance(base_val, dict):
        return base_val
    # broadcast scalar / fallback
    try:
        scalar = float(base_val)
    except Exception:
        scalar = 1.0
    return {k: scalar for k in factor_keys}

# ---------- Helpers ----------


def _config_paths():
    """Resolve config files relative to this module so the CLI works anywhere."""
    here = os.path.dirname(__file__)  # .../src/ets
    cfg_dir = os.path.join(here, "config")
    return (
        os.path.join(cfg_dir, "config.yaml"),
        os.path.join(cfg_dir, "weights.yaml"),
    )


def parse_args():
    ap = argparse.ArgumentParser(description="ETS: Pre-earnings MVP (free-tier)")
    ap.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date YYYY-MM-DD (default: today)",
    )
    ap.add_argument(
        "--session",
        default="amc",
        choices=["amc", "bmo"],
        help="Earnings session: amc (after market close) or bmo (before market open)",
    )
    ap.add_argument(
        "--tickers",
        help="Path to CSV with tickers (one per line). If omitted, tries Finnhub calendar.",
    )
    return ap.parse_args()


def load_configs():
    cfg_path, wts_path = _config_paths()

    # config.yaml (optional)
    cfg = load_yaml(cfg_path)
    cfg = deep_merge(DEFAULT_CFG, cfg or {})

    # weights.yaml (optional) – DEFAULT_WEIGHTS comes from ets.config.defaults
    # If file exists and has content, merge over defaults; otherwise keep defaults.
    wts_file = load_yaml(wts_path)
    wts = deep_merge(DEFAULT_WEIGHTS, wts_file or {})

    # Ensure required keys exist so downstream code can .get(...) safely
    if "base" not in wts:
        wts["base"] = 1.0
    try:
        wts["base"] = float(wts.get("base", 1.0))
    except Exception:
        wts["base"] = 1.0
    wts.setdefault("caps", {})
    wts.setdefault("multipliers", {})

    # Create app dirs
    ensure_dirs(
        cfg["app"]["out_dir"],
        cfg["app"]["logs_dir"],
        cfg["app"]["cache_dir"],
    )
    return cfg, wts


def _load_sector_map_from_cache(cache_dir: str = "cache") -> dict:
    """
    Read cache/sectors.csv if present. Format: TICKER,SECTOR (no header required).
    Returns dict like {'AAPL': 'Information Technology', ...}
    """
    path = os.path.join(cache_dir, "sectors.csv")
    mapping = {}
    if os.path.exists(path):
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                rdr = csv.reader(f)
                for row in rdr:
                    if not row:
                        continue
                    t = str(row[0]).strip().upper()
                    s = (row[1] if len(row) > 1 else "Unknown").strip()
                    if t:
                        mapping[t] = s
        except Exception:
            pass
    return mapping


def build_universe(date_str: str, tickers_csv: str | None, provider_reg: ProviderRegistry | None):
    if tickers_csv:
        syms = from_csv(tickers_csv)
    else:
        # Use Finnhub if available; otherwise empty
        syms = from_finnhub(date_str)
    return syms or []


# ---------- Main ----------


def main():
    args = parse_args()
    cfg, wts = load_configs()

    # Optional: session-specific weights override if present
    try:
        sess = getattr(args, "session", None)
        if isinstance(wts, dict) and isinstance(wts.get("by_session"), dict):
            if sess in wts["by_session"]:
                wts = deep_merge(wts, wts["by_session"][sess] or {})
                # re-ensure base is numeric
                wts["base"] = float(wts.get("base", 1.0))
    except Exception:
        pass

    # Initialize providers (Finnhub-first) and attach to quotes aggregator
    load_env()
    try:
        require_env("FINNHUB_API_KEY", "FINNHUB_API_KEY missing. Put it in .env")
    except Exception as e:
        print("[WARN]", e)
    reg = ProviderRegistry(cfg)
    set_registry(reg)
    set_calendar_registry(reg)

    rid = run_id(args.date, args.session)
    out_dir = cfg["app"]["out_dir"]

    # Sector map: prefer cache/sectors.csv; do NOT require config keys
    sector_map = _load_sector_map_from_cache(cfg["app"]["cache_dir"])

    # 1) Universe
    universe = build_universe(args.date, args.tickers, reg)
    set_fallback_peers(list(universe))
    if not universe:
        print("No tickers found from calendar or CSV. Provide --tickers path.")
        sys.exit(0)

    # 2) Raw factors (gentle throttle to reduce upstream 429s even with limiter)
    raw_rows = []
    for idx, t in enumerate(universe):
        row = compute_raw_factors(t, sector_map)
        if row:
            raw_rows.append(row)
        # very light sleep to avoid synchronized bursts across providers
        if idx % 3 == 0:
            time.sleep(0.20)

    if not raw_rows:
        print("No factor rows computed.")
        sys.exit(0)

    factors_df = pd.DataFrame(raw_rows)

    # Defensive aliases to smooth legacy/new column naming
    if "price" not in factors_df.columns and "last" in factors_df.columns:
        factors_df["price"] = factors_df["last"]
    if "dollar_vol" not in factors_df.columns:
        if "dollar_volume" in factors_df.columns:
            factors_df["dollar_vol"] = factors_df["dollar_volume"]
        elif {"last", "volume"}.issubset(factors_df.columns):
            factors_df["dollar_vol"] = factors_df["last"] * factors_df["volume"]
        else:
            factors_df["dollar_vol"] = 0.0

    # 3) Normalize
    norm_cols = [
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
    factors_df = robust_normalize_df(
        factors_df,
        cols=norm_cols,
        p_low=cfg["normalization"]["winsor_p_low"],
        p_high=cfg["normalization"]["winsor_p_high"],
        min_universe_for_robust=cfg["normalization"]["min_universe_for_robust"],
        clip_min=cfg["normalization"]["clip_min"],
        clip_max=cfg["normalization"]["clip_max"],
    )
    # rename *_raw_norm -> *_norm
    rename_map = {c + "_norm": c.replace("_raw", "") + "_norm" for c in norm_cols}
    factors_df.rename(columns=rename_map, inplace=True)

    # 4) Score
    df_scores = compute_scores(
        factors_df,
        base=_coerce_base(wts.get("base", 1.0), norm_cols),
        mult=wts.get("multipliers", {}),
        caps=wts.get("caps", {}),
    )

    # 5) Select
    sel_cfg = cfg["selection"]
    uni_cfg = cfg["universe"]
    df_trades = apply_filters_and_select(
        df_scores,
        min_price=uni_cfg["min_price"],
        dollar_volume_floor=uni_cfg["dollar_volume_floor"],
        risk_floor_sigma_g=sel_cfg["risk_floor_sigma_g"],
        risk_floor_tau_g=sel_cfg["risk_floor_tau_g"],
        score_threshold=sel_cfg["score_threshold"],
        max_names=sel_cfg["max_names"],
        max_per_sector=sel_cfg["max_per_sector"],
    )

    # 6) Write outputs (including pull & provider telemetry)
    factors_path = os.path.join(out_dir, f"{rid}_factors.csv")
    scores_path = os.path.join(out_dir, f"{rid}_scores.csv")
    trades_path = os.path.join(out_dir, f"{rid}_trades.csv")
    pulls_path = os.path.join(out_dir, f"{rid}_pulls.csv")
    tele_path = os.path.join(out_dir, f"{rid}_telemetry.csv")

    write_factors(factors_path, raw_rows)
    df_scores.to_csv(scores_path, index=False)
    df_trades.to_csv(trades_path, index=False)
    write_pulls(pulls_path, get_pull_log())
    write_telemetry(tele_path, reg.stats())

    # write trader-friendly clean file
    finalize_results(scores_path, out_dir)

    print(
        f"[OK] Wrote:\n  {factors_path}\n  {scores_path}\n  {trades_path}\n  {pulls_path}\n  {tele_path}"
    )


if __name__ == "__main__":
    main()
