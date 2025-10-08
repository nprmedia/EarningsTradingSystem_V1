from typing import List, Dict


def session_flag(event_obj: Dict) -> str:
    return event_obj.get("session", "amc")


def calendar_density(events: List[Dict], day: str) -> int:
    return sum(1 for e in events if e.get("date") == day)
