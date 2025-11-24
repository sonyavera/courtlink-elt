"""Timezone helpers to keep ingestion pipelines consistent."""

from __future__ import annotations

from typing import Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DEFAULT_TIMEZONE = "UTC"

# Keep empty until we introduce clients that emit local times.
CLIENT_TIMEZONE_OVERRIDES = {}


def resolve_timezone(
    *,
    explicit: Optional[str] = None,
    client_code: Optional[str] = None,
    default: str = DEFAULT_TIMEZONE,
) -> Tuple[str, ZoneInfo]:
    """
    Determine which timezone to associate with an incoming record.

    Preference order:
        1. Explicit timezone string from the payload
        2. Known client-level override
        3. Provided default
        4. System default (UTC)
    """
    candidates = []

    if explicit:
        candidates.append(explicit)

    if client_code:
        override = CLIENT_TIMEZONE_OVERRIDES.get(client_code.lower())
        if override:
            candidates.append(override)

    candidates.append(default)
    candidates.append(DEFAULT_TIMEZONE)

    for candidate in candidates:
        if not candidate:
            continue
        try:
            return candidate, ZoneInfo(candidate)
        except ZoneInfoNotFoundError:
            continue

    # Final fallback in case ZoneInfo lacks UTC (should never happen)
    return DEFAULT_TIMEZONE, ZoneInfo(DEFAULT_TIMEZONE)

