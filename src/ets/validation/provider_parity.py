from __future__ import annotations
import os
import json
import time
import pathlib
from typing import Dict, Any, List
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = (ROOT.parent / "out") if not (ROOT / "out").exists() else (ROOT / "out")
MET = (
    (ROOT.parent / "metrics") if not (ROOT / "metrics").exists() else (ROOT / "metrics")
)
FIX = (
    (ROOT.parent / "tests" / "fixtures")
    if (ROOT / "tests" / "fixtures").exists()
    else (ROOT.parent / "tests" / "fixtures")
)


# -------- helpers --------
def _load_mock(symbols: List[str]) -> pd.DataFrame:
    data = json.loads((FIX / "mock_quotes.json").read_text())
    rows = []
    for s in symbols:
        d = data.get(s.upper())
        if not d:
            continue
        rows.append(
            {
                "provider": "mock",
                "symbol": s.upper(),
                "open": float(d["open"]),
                "high": float(d["high"]),
                "low": float(d["low"]),
                "close": float(d["close"]),
                "last": float(d["last"]),
                "volume": int(d["volume"]),
            }
        )
    return pd.DataFrame(rows)


def _load_yahoo(symbols: List[str]) -> pd.DataFrame:
    import yfinance as yf

    rows = []
    for s in symbols:
        t = yf.Ticker(s)
        q = t.fast_info if hasattr(t, "fast_info") else {}
        last = float(q.get("last_price") or q.get("last_price", 0.0) or 0.0)
        # Try history fallback for OHLC
        hist = t.history(period="1d")
        if not hist.empty:
            o = float(hist["Open"].iloc[-1])
            h = float(hist["High"].iloc[-1])
            low = float(hist["Low"].iloc[-1])
            c = float(hist["Close"].iloc[-1])
            v = int(hist["Volume"].iloc[-1])
        else:
            o = h = low = c = last
            v = 0
        rows.append(
            {
                "provider": "yahoo",
                "symbol": s.upper(),
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "last": last,
                "volume": v,
            }
        )
    return pd.DataFrame(rows)


def _load_finnhub(symbols: List[str]) -> pd.DataFrame:
    import finnhub

    key = os.getenv("FINNHUB_API_KEY", "")
    client = finnhub.Client(api_key=key) if key else None
    rows = []
    for s in symbols:
        if not client:
            # no key -> empty row (will be handled by mock-only run)
            continue
        q = client.quote(s)
        # finnhub fields: o,h,low,c,pc,t
        o = float(q.get("o") or 0.0)
        h = float(q.get("h") or 0.0)
        low = float(q.get("low") or 0.0)
        c = float(q.get("c") or 0.0)
        v = float(q.get("t") or 0.0)  # t is timestamp; live volume not provided -> 0
        rows.append(
            {
                "provider": "finnhub",
                "symbol": s.upper(),
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "last": c,
                "volume": int(v or 0),
            }
        )
    return pd.DataFrame(rows)


def _schema(df: pd.DataFrame) -> Dict[str, str]:
    return {col: str(df[col].dtype) for col in sorted(df.columns)}


def _relative_diff(a: float, b: float) -> float:
    if a == b:
        return 0.0
    denom = abs(a) if abs(a) > 1e-9 else 1.0
    return abs(a - b) / denom


# -------- main validate --------
def validate_providers(symbols: List[str], mode: str = "mock") -> Dict[str, Any]:
    OUT.mkdir(parents=True, exist_ok=True)
    MET.mkdir(parents=True, exist_ok=True)
    providers = []
    if mode == "mock":
        providers = ["mock", "yahoo"]  # compare mock vs yahoo locally
    else:
        providers = ["finnhub", "yahoo"]

    frames = {}
    latencies = {}
    for p in providers:
        t0 = time.perf_counter()
        if p == "mock":
            df = _load_mock(symbols)
        elif p == "yahoo":
            df = _load_yahoo(symbols)
        elif p == "finnhub":
            df = _load_finnhub(symbols)
        else:
            continue
        latencies[p] = time.perf_counter() - t0
        if not df.empty:
            frames[p] = df.sort_values("symbol").reset_index(drop=True)

    # Schema parity
    schemas = {p: _schema(df) for p, df in frames.items()}
    all_cols = sorted({c for df in frames.values() for c in df.columns})
    schema_diff = {}
    for p, sch in schemas.items():
        missing = [c for c in all_cols if c not in sch]
        extras = [c for c in sch if c not in all_cols]  # always none
        schema_diff[p] = {"missing": missing, "extras": extras}

    # Value range report vs first provider as baseline
    value_report = {"baseline": None, "comparisons": []}
    if frames:
        base_name = list(frames.keys())[0]
        base = frames[base_name].set_index("symbol")
        value_report["baseline"] = base_name
        for p, df in frames.items():
            if p == base_name:
                continue
            comp = df.set_index("symbol")
            syms = sorted(set(base.index).intersection(comp.index))
            breaches = []
            for s in syms:
                for col in ["open", "high", "low", "close", "last"]:
                    a = float(base.at[s, col])
                    b = float(comp.at[s, col])
                    rd = _relative_diff(a, b)
                    if rd > 0.25:  # >25% deviation
                        breaches.append(
                            {
                                "symbol": s,
                                "field": col,
                                "baseline": a,
                                "candidate": b,
                                "rel_diff": rd,
                            }
                        )
            value_report["comparisons"].append(
                {"against": p, "breaches": breaches, "symbols": syms}
            )

    # Save artifacts
    (OUT / "provider_schema_diff.json").write_text(
        json.dumps({"schemas": schemas, "diff": schema_diff}, indent=2)
    )
    (OUT / "provider_range_report.json").write_text(json.dumps(value_report, indent=2))
    (MET / "provider_perf.json").write_text(
        json.dumps({"latency_sec": latencies, "providers": providers}, indent=2)
    )
    return {
        "schemas": schemas,
        "diff": schema_diff,
        "values": value_report,
        "latency": latencies,
    }
