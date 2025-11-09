from datetime import datetime
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")


def parse_event_time(date_str: str) -> datetime:
    if not date_str:
        return None
    dt = datetime.fromisoformat(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EASTERN)
    return dt


def parse_utc_time(date_str: str) -> datetime:
    if not date_str:
        return None
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return dt.astimezone(EASTERN)
