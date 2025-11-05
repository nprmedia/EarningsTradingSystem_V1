import os

import pandas as pd


def write_telemetry(path: str, provider_stats: list[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(provider_stats)
    df.to_csv(path, index=False)
