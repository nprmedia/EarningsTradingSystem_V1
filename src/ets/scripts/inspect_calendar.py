import argparse, sys
from datetime import date
try:
    from ets.scripts import build_universe as bu
except Exception as e:
    print("[WARN] cannot import build_universe:", e)
    sys.exit(0)

ap = argparse.ArgumentParser()
ap.add_argument("--days", type=int, default=2)
ap.add_argument("--session", type=str, default="amc")
args = ap.parse_args()

prefetch = getattr(bu, "prefetch_main", None) or getattr(bu, "prefetch", None)
if not prefetch:
    print("[WARN] no prefetch function found")
    sys.exit(0)
try:
    res = prefetch(days=args.days)
    n = len(res) if hasattr(res, "__len__") else -1
    print(f"[INFO] Prefetch returned {n} rows")
    it = iter(res)
    for i, x in enumerate(it):
        if i >= 50: break
        print(x)
except Exception as e:
    print("[WARN] prefetch failed:", e)
