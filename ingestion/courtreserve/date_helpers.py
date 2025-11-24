from datetime import datetime
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")


def parse_event_time(date_str: str) -> datetime:
    if not date_str:
        return None
    normalized = date_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EASTERN)
    return dt.astimezone(UTC)


def parse_utc_time(date_str: str) -> datetime:
    if not date_str:
        return None
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
