import os, csv, requests

def from_finnhub(date_str: str) -> list:
    token = os.getenv("FINNHUB_TOKEN")
    if not token: return []
    url = f"https://finnhub.io/api/v1/calendar/earnings?from={date_str}&to={date_str}&token={token}"
    try:
        r = requests.get(url, timeout=10); r.raise_for_status()
        data = r.json().get("earningsCalendar", [])
        syms = []
        for row in data:
            sym = (row.get("symbol") or "").upper().strip()
            if sym: syms.append(sym)
        return sorted(list(set(syms)))
    except Exception:
        return []

def from_csv(path: str) -> list:
    out=[]
    try:
        with open(path, "r", newline="") as f:
            for row in csv.reader(f):
                if not row: continue
                t = row[0].strip().upper()
                if t and t!="TICKER": out.append(t)
        return sorted(list(set(out)))
    except Exception:
        return []
