_RUN_DATE = None  # "YYYY-MM-DD"

def set_run_date(ds: str):
    global _RUN_DATE
    _RUN_DATE = str(ds)

def get_run_date(default: str | None = None) -> str:
    if _RUN_DATE:
        return _RUN_DATE
    return default or ""
