from __future__ import annotations
import argparse
import subprocess
import sys

FACTOR_TO_MODULE = {
    "M_raw": "ets.scripts.build_factor_m_raw",
    "V_raw": "ets.scripts.build_factor_v_raw",
    "CAL_raw": "ets.scripts.build_factor_cal_raw",
    "A_raw": "ets.scripts.build_factor_a_raw",
    "sigma_raw": "ets.scripts.build_factor_sigma_raw",
    "tau_raw": "ets.scripts.build_factor_tau_raw",
    "TREND_raw": "ets.scripts.build_factor_trend_raw",
    "SRM_raw": "ets.scripts.build_factor_srm_raw",
    "VIX_raw": "ets.scripts.build_factor_vix_raw",
    "PEER_raw": "ets.scripts.build_factor_peer_raw",
    "ETFF_raw": "ets.scripts.build_factor_etff_raw",
    # add more mappings here as you implement them
}


def main():
    ap = argparse.ArgumentParser(
        description="Build multiple factors into out/factors_latest.csv"
    )
    ap.add_argument(
        "--factors",
        required=True,
        help="Comma-separated list, e.g. M_raw,V_raw,CAL_raw",
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    args = ap.parse_args()

    factors = [f.strip() for f in args.factors.split(",") if f.strip()]
    for f in factors:
        mod = FACTOR_TO_MODULE.get(f)
        if not mod:
            print(f"[WARN] Unknown factor '{f}' (skip)")
            continue
        cmd = [sys.executable, "-m", mod]
        if args.symbols:
            cmd += ["--symbols", args.symbols]
        print("[RUN]", " ".join(cmd))
        rc = subprocess.call(cmd)
        if rc != 0:
            print(f"[ERROR] {f} builder exited {rc}", file=sys.stderr)
            sys.exit(rc)
    print("[DONE] factors build complete")


if __name__ == "__main__":
    main()
