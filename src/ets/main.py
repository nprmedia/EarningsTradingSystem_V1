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
from ets.outputs.csv_writer import write_factors, write_pulls
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
    factor_keys = [c.replace("_raw", "") for c in (norm_cols or [])]
    if not factor_keys:
        factor_keys = [
            "M",
            "V",
            "S",
            "A",
            "sigma",
            "tau",
            "CAL",
            "SRM",
            "PEER",
            "ETFF",
            "VIX",
            "TREND",
        ]
    if isinstance(base_val, dict):
        return base_val
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
    ap.add_argument(
        "--dry",
        action="store_true",
        help="Run deterministic pipeline without live API calls or writes.",
    )
    return ap.parse_args()


def load_configs():
    cfg_path, wts_path = _config_paths()

    # config.yaml (optional)
    cfg = load_yaml(cfg_path)
    cfg = deep_merge(DEFAULT_CFG, cfg or {})

    # weights.yaml (optional)
    wts_file = load_yaml(wts_path)
    wts = deep_merge(DEFAULT_WEIGHTS, wts_file or {})

    if "base" not in wts:
        wts["base"] = 1.0
    try:
        wts["base"] = float(wts.get("base", 1.0))
    except Exception:
        wts["base"] = 1.0
    wts.setdefault("caps", {})
    wts.setdefault("multipliers", {})

    ensure_dirs(cfg["app"]["out_dir"], cfg["app"]["logs_dir"], cfg["app"]["cache_dir"])
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


def build_universe(
    date_str: str, tickers_csv: str | None, provider_reg: ProviderRegistry | None
):
    if tickers_csv:
        syms = from_csv(tickers_csv)
    else:
        syms = from_finnhub(date_str)
    return syms or []


# ---------- Tiered Strictness Gate ----------


def _phase2_gate(factors_df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase-2 completeness (Tiered Strictness):
    - Require only CORE free, reliable factors
    - Allow OPTIONAL factors; do not fail if missing
    """
    CORE = [
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
    OPTIONAL = [
        "EPSG_raw",
        "ROE_raw",
        "PEG_raw",
        "DE_raw",
        "SI_raw",
        "OPT_SK_raw",
        "INSIDER_raw",
        "NEWS_raw",
        "RISK_APP_raw",
        "LIQ_raw",
        "EAT_raw",
    ]

    # Structural check: all CORE columns must exist in the frame
    missing_cols = [c for c in CORE if c not in factors_df.columns]
    if missing_cols:
        raise SystemExit(
            f"[FATAL] Missing required CORE factor columns: {missing_cols}"
        )

    # Drop rows with any null in CORE factors (strict)
    before = len(factors_df)
    factors_df = factors_df.dropna(subset=CORE)
    dropped = before - len(factors_df)
    if dropped > 0:
        print(f"[INFO] Dropped {dropped} ticker(s) lacking full CORE factor coverage.")

    # Optional coverage stats (purely informative)
    opt_present = [c for c in OPTIONAL if c in factors_df.columns]
    if opt_present:
        factors_df["_optional_count"] = factors_df[opt_present].notna().sum(axis=1)
        present_n = len(opt_present)
        mean_cov = (
            float(factors_df["_optional_count"].mean()) if len(factors_df) else 0.0
        )
        print(
            f"[INFO] OPTIONAL factors available this run: {present_n} -> {opt_present}"
        )
        print(f"[INFO] Mean OPTIONAL coverage per ticker: {mean_cov:.2f}/{present_n}")
    else:
        print("[INFO] No OPTIONAL factors available this run (still OK).")

    return factors_df


# ---------- Main ----------


def main():
    args = parse_args()
    cfg, wts = load_configs()

    dry_run = getattr(args, "dry", False)
    if dry_run:
        print("[DRY] Running in dry-run mode — no network calls or file writes.")
        os.environ["ETS_MODE"] = "dry"

    # Optional: session-specific weights override if present
    try:
        sess = getattr(args, "session", None)
        if isinstance(wts, dict) and isinstance(wts.get("by_session"), dict):
            if sess in wts["by_session"]:
                wts = deep_merge(wts, wts["by_session"][sess] or {})
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
        return 0

    # 2) Raw factors (gentle throttle to reduce upstream 429s even with limiter)
    raw_rows = []
    for idx, t in enumerate(universe):
        row = compute_raw_factors(t, sector_map)
        if row:
            raw_rows.append(row)
        if idx % 3 == 0:
            time.sleep(0.20)

    if not raw_rows:
        print("No factor rows computed.")
        return 0

    factors_df = pd.DataFrame(raw_rows)

    # 2b) Phase-2 completeness gate (Tiered Strictness)
    factors_df = _phase2_gate(factors_df)

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

    # 3) Normalize — dynamic column list (CORE first, then any other *_raw present)
    norm_cols = [c for c in factors_df.columns if c.endswith("_raw")]
    CORE_ORDER = [
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
    core_in = [c for c in CORE_ORDER if c in norm_cols]
    rest = [c for c in norm_cols if c not in CORE_ORDER]
    norm_cols = core_in + sorted(rest)

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

    if not dry_run:
        write_factors(factors_path, raw_rows)
    df_scores.to_csv(scores_path, index=False)
    df_trades.to_csv(trades_path, index=False)
    if not dry_run:
        write_pulls(pulls_path, get_pull_log())
        write_telemetry(tele_path, reg.stats())
        finalize_results(scores_path, out_dir)

    print(
        f"[OK] Wrote:\n  {factors_path}\n  {scores_path}\n  {trades_path}\n  {pulls_path}\n  {tele_path}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
