"""Datetime parsing helpers shared across ingestion pipelines."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    normalized = value.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def format_date(value: Optional[datetime]) -> Optional[date]:
    if not value:
        return None
    return value.date()


def day_of_week(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    return value.strftime("%A")

