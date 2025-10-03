import os, yaml, math
from datetime import datetime
from dateutil import tz

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_id(date_str: str, session: str) -> str:
    return f"{date_str.replace('-','')}_{session}"

def get_timezone(tz_name: str = None):
    tzname = tz_name or os.getenv("APP_TZ", "America/Chicago")
    return tz.gettz(tzname)

def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def median(values):
    arr = sorted([v for v in values if v is not None])
    n = len(arr)
    if n == 0: return None
    mid = n // 2
    if n % 2 == 1: return arr[mid]
    return 0.5 * (arr[mid-1] + arr[mid])

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)
