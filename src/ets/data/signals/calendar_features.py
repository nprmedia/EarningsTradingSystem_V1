def session_flag(event_obj: dict) -> str:
    return event_obj.get("session", "amc")


def calendar_density(events: list[dict], day: str) -> int:
    return sum(1 for e in events if e.get("date") == day)
