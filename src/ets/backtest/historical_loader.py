from __future__ import annotations
import pandas as pd
from pathlib import Path


def load_signals(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"])
    # expected columns: date,ticker,sector,score,rank,signal  (signal ∈ {1,-1})
    req = {"date", "ticker", "signal"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"signals missing columns: {sorted(miss)}")
    df["ticker"] = df["ticker"].str.upper()
    return df


def load_history(path: Path) -> pd.DataFrame:
    hist = pd.read_csv(path, parse_dates=["date"])
    # expected columns: date,ticker,close
    req = {"date", "ticker", "close"}
    miss = req - set(hist.columns)
    if miss:
        raise ValueError(f"history missing columns: {sorted(miss)}")
    hist["ticker"] = hist["ticker"].str.upper()
    return hist


def make_panel(signals: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    # join next-day close to compute forward return
    history = history.sort_values(["ticker", "date"]).copy()
    history["close_next"] = history.groupby("ticker")["close"].shift(-1)
    panel = signals.merge(history, on=["date", "ticker"], how="left")
    # find next-day close by merging on next day after signal date
    nxt = history[["ticker", "date", "close_next"]].copy()
    nxt = nxt.rename(columns={"date": "date_for_next"})
    # align date_for_next = signal date
    panel = panel.merge(
        nxt,
        left_on=["ticker", "date"],
        right_on=["ticker", "date_for_next"],
        how="left",
    )
    panel["ret_next"] = (panel["close_next"] - panel["close"]) / panel["close"]
    return panel.drop(columns=["date_for_next"])
