import os
import pandas as pd

def write_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)

def write_factors(path: str, rows: list):
    df = pd.DataFrame(rows)
    write_csv(df, path)

def write_scores(path: str, df_scores: pd.DataFrame):
    write_csv(df_scores, path)

def write_trades(path: str, df_trades: pd.DataFrame):
    write_csv(df_trades, path)

def write_pulls(path: str, rows: list):
    df = pd.DataFrame(rows)
    write_csv(df, path)
