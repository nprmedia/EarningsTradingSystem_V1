from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def _col(df: pd.DataFrame, names: Sequence[str], default=None):
    """Return the first existing column from names, else default."""
    for n in names:
        if n in df.columns:
            return df[n]
    return default


def _ensure_dollar_vol(df: pd.DataFrame) -> pd.Series:
    # Prefer explicit columns; else try compute from last/price * volume
    s = _col(df, ["dollar_vol", "dollar_volume"])
    if s is not None:
        return s.fillna(0.0)
    last = _col(df, ["price", "last", "close", "c"])
    vol = _col(df, ["volume", "vol"])
    if last is not None and vol is not None:
        return (last * vol).fillna(0.0)
    return pd.Series(0.0, index=df.index)


def _ensure_price(df: pd.DataFrame) -> pd.Series:
    s = _col(df, ["price", "last", "close", "c"])
    if s is not None:
        return s.fillna(0.0)
    return pd.Series(0.0, index=df.index)


def _ensure_sigma(df: pd.DataFrame) -> pd.Series:
    s = _col(df, ["sigma_g", "sigma_norm", "sigma"])
    if s is not None:
        return s.fillna(0.0)
    return pd.Series(0.0, index=df.index)


def _ensure_tau(df: pd.DataFrame) -> pd.Series:
    s = _col(df, ["tau_g", "tau_norm", "tau"])
    if s is not None:
        return s.fillna(0.0)
    return pd.Series(0.0, index=df.index)


def apply_filters_and_select(
    df_scores: pd.DataFrame,
    *,
    min_price: float,
    dollar_volume_floor: float,
    risk_floor_sigma_g: float,
    risk_floor_tau_g: float,
    score_threshold: float,
    max_names: int,
    max_per_sector: int,
) -> pd.DataFrame:
    df = df_scores.copy()

    # Ensure minimally required series (no KeyErrors)
    price = _ensure_price(df)
    dvol = _ensure_dollar_vol(df)
    sigma = _ensure_sigma(df)
    tau = _ensure_tau(df)

    # Sector column optional — if missing, assign "Unknown"
    if "sector" not in df.columns:
        df["sector"] = "Unknown"

    # Apply base filters
    ok = (
        (price >= float(min_price))
        & (dvol >= float(dollar_volume_floor))
        & (sigma >= float(risk_floor_sigma_g))
        & (tau >= float(risk_floor_tau_g))
        & (df.get("score", 0.0) >= float(score_threshold))
    )

    filtered = df[ok].copy()

    if filtered.empty:
        return filtered

    # Rank by score desc
    filtered = filtered.sort_values("score", ascending=False)

    # Sector caps
    if max_per_sector and max_per_sector > 0:
        filtered = filtered.groupby("sector", group_keys=False).head(int(max_per_sector))

    # Global cap
    if max_names and max_names > 0:
        filtered = filtered.head(int(max_names))

    return filtered.reset_index(drop=True)
