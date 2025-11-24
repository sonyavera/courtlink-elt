"""Datetime parsing helpers shared across ingestion pipelines."""

from __future__ import annotations

from datetime import date, datetime, timezone, tzinfo
from typing import Optional, Union


DatetimeInput = Union[str, datetime]


def parse_iso_datetime(
    value: Optional[str], assume_timezone: tzinfo = timezone.utc
) -> Optional[datetime]:
    """
    Parse an ISO-8601 string and return a UTC datetime.

    Args:
        value: ISO-8601 formatted datetime string.
        assume_timezone: tzinfo to use when the string is naive.
    """
    return to_utc_datetime(value, assume_timezone)


def to_utc_datetime(
    value: Optional[DatetimeInput], assume_timezone: tzinfo = timezone.utc
) -> Optional[datetime]:
    """
    Coerce a datetime-like value into an aware UTC datetime.

    Args:
        value: ISO string or datetime instance.
        assume_timezone: tzinfo to apply when the value is naive.
    """
    if not value:
        return None

    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    elif isinstance(value, datetime):
        parsed = value
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=assume_timezone)

    return parsed.astimezone(timezone.utc)


def format_date(value: Optional[datetime]) -> Optional[date]:
    if not value:
        return None
    return value.date()


def day_of_week(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    return value.strftime("%A")

