from __future__ import annotations
import argparse
import time
import json
import yaml
import re
from pathlib import Path
from typing import Any, Dict, Set, List
import pandas as pd

# Local ETS imports
from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.fetchers import fetch_factor

SYMBOL_CLEAN_RE = re.compile(r"[^A-Za-z0-9.\-\^]")


def normalize_symbol(s: str) -> str:
    s = (s or "").strip().upper()
    if s.startswith("$"):
        s = s[1:]
    return SYMBOL_CLEAN_RE.sub("", s)


def load_config() -> Dict[str, Any]:
    # Try common locations; your repo uses src/ets/config/config.yaml
    here = Path(__file__).resolve()
    for rel in [
        Path("src/ets/config/config.yaml"),
        Path("ets/config/config.yaml"),
        Path("config/config.yaml"),
    ]:
        cand = here.parents[2] / rel
        if cand.exists():
            return yaml.safe_load(cand.read_text())
    raise FileNotFoundError("config.yaml not found")


def load_symbols(csv_path: Path) -> List[str]:
    df = pd.read_csv(csv_path)
    col = df.columns[0]
    syms = [normalize_symbol(str(x)) for x in df[col].dropna().astype(str)]
    return [s for s in syms if s]


def load_processed(jsonl_path: Path) -> Set[str]:
    done: Set[str] = set()
    if jsonl_path.exists():
        with jsonl_path.open() as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if obj.get("data"):
                        done.add(str(obj.get("symbol", "")).upper())
                except Exception:
                    continue
    return done


def append_results(jsonl_path: Path, rows: List[Dict[str, Any]]) -> None:
    with jsonl_path.open("a") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")


def write_csv_summary(jsonl_path: Path, csv_out: Path) -> None:
    # Build a minimal CSV snapshot each cycle
    rows = []
    if jsonl_path.exists():
        with jsonl_path.open() as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                d = obj.get("data") or {}
                rows.append(
                    {
                        "symbol": obj.get("symbol"),
                        "c": d.get("c"),
                        "h": d.get("h"),
                        "l": d.get("l"),
                        "o": d.get("o"),
                        "pc": d.get("pc"),
                        "t": d.get("t"),
                        "_src": d.get("_src"),
                    }
                )
    pd.DataFrame(rows).drop_duplicates(subset=["symbol"], keep="last").to_csv(
        csv_out, index=False
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?", default="tickers.csv")
    ap.add_argument("--factor", default="quote")
    ap.add_argument(
        "--follow",
        action="store_true",
        help="watch tickers.csv for changes and keep processing",
    )
    ap.add_argument(
        "--cycle-sec", type=int, default=15, help="poll interval when --follow is set"
    )
    args = ap.parse_args()

    cfg = load_config()
    out_dir = Path(cfg["app"]["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = out_dir / f"{args.factor}_results.jsonl"
    csv_out = out_dir / f"{args.factor}_results.csv"

    reg = ProviderRegistry(cfg)

    # Informative headroom print (reserve from YAML)
    try:
        reserve = reg.finnhub["limiter"].reserve  # type: ignore[index]
    except Exception:
        reserve = "NA"
    print(f"[INFO] factor={args.factor} out={out_dir} finnhub_reserve={reserve}")

    last_mtime = None
    processed = load_processed(jsonl_path)  # resume support

    while True:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            print(f"[WARN] {csv_path} not found; sleeping...")
            time.sleep(max(5, args.cycle_sec))
            if not args.follow:
                break
            continue

        # Reload on change (or first run)
        mtime = csv_path.stat().st_mtime
        if last_mtime is None or mtime != last_mtime:
            symbols = load_symbols(csv_path)
            last_mtime = mtime
        else:
            symbols = load_symbols(
                csv_path
            )  # still reload; file content may change without mtime in some FS

        # Compute worklist (skip already successful)
        todo = [s for s in symbols if s not in processed]
        print(f"[CYCLE] total={len(symbols)} todo={len(todo)} done={len(processed)}")

        cycle_rows: List[Dict[str, Any]] = []
        ok = fail = 0

        for i, sym in enumerate(todo, 1):
            data = fetch_factor(reg, args.factor, sym)  # blocks under rate limits
            if data:
                ok += 1
                processed.add(sym)
                cycle_rows.append({"symbol": sym, "data": data})
            else:
                fail += 1
                cycle_rows.append({"symbol": sym, "data": None})

            if i % 25 == 0:
                try:
                    head = reg.finnhub["limiter"].get_stats()["windows"][0]["headroom"]  # type: ignore[index]
                except Exception:
                    head = "NA"
                print(f"[PROGRESS] {i}/{len(todo)} ok={ok} fail={fail} headroom={head}")

            # Flush incrementally to be crash-safe
            if len(cycle_rows) >= 100:
                append_results(jsonl_path, cycle_rows)
                write_csv_summary(jsonl_path, csv_out)
                cycle_rows.clear()

        # Final flush for this cycle
        if cycle_rows:
            append_results(jsonl_path, cycle_rows)
            write_csv_summary(jsonl_path, csv_out)

        print(
            f"[DONE-CYCLE] ok={ok} fail={fail} total_done={len(processed)} out={csv_out}"
        )

        if not args.follow:
            break
        time.sleep(max(5, args.cycle_sec))


if __name__ == "__main__":
    main()
