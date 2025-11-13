from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from dotenv import load_dotenv

from constants import EltWatermarks, Tables
from ingestion.clients import CourtReserveClient, PodplayClient, PostgresClient
from ingestion.courtreserve.member_mapper import map_member_to_row as map_cr_member
from ingestion.courtreserve.reservation_helpers import (
    normalize_reservations as normalize_cr_reservations,
)
from ingestion.courtreserve.reservation_cancellation_helpers import (
    normalize_reservation_cancellations as normalize_cr_cancellations,
)
from ingestion.podplay.members import normalize_members as normalize_podplay_members
from ingestion.podplay.reservations import (
    normalize_event_reservations as normalize_podplay_reservations,
)

load_dotenv()

pg_schema = os.getenv("PG_SCHEMA")
pg_dsn = os.getenv("PG_DSN")

if not pg_schema or not pg_dsn:
    raise RuntimeError(
        "Postgres DSN and schema must be configured via PG_DSN and PG_SCHEMA"
    )

pg_client = PostgresClient(pg_dsn, pg_schema)

_courtreserve_clients: Dict[str, CourtReserveClient] = {}
_podplay_clients: Dict[str, PodplayClient] = {}

DEFAULT_LOOKBACK_DAYS = int(os.getenv("DEFAULT_LOOKBACK_DAYS", "30"))


def _get_sample_size() -> Optional[int]:
    raw = os.getenv("INGEST_SAMPLE_SIZE")
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _get_courtreserve_client(client_code: str) -> CourtReserveClient:
    global _courtreserve_clients
    code = client_code.lower()
    if code not in _courtreserve_clients:
        user_env = f"CR_API_USER_{code.upper()}"
        pw_env = f"CR_API_PW_{code.upper()}"
        username = os.getenv(user_env) or os.getenv("CR_API_USER")
        password = os.getenv(pw_env) or os.getenv("CR_API_PW")
        if not username or not password:
            raise RuntimeError(
                f"CourtReserve credentials are not configured for client '{client_code}'. "
                f"Set {user_env}/{pw_env} or CR_API_USER/CR_API_PW."
            )
        _courtreserve_clients[code] = CourtReserveClient(username, password)
    return _courtreserve_clients[code]


def _get_courtreserve_client_codes() -> list[str]:
    raw_codes = os.getenv("CR_CLIENT_CODES", "pklyn")
    codes = [c.strip().lower() for c in raw_codes.split(",") if c.strip()]
    if not codes:
        raise RuntimeError(
            "CR_CLIENT_CODES is empty; configure at least one client code."
        )
    return codes


def _get_podplay_client(client_code: str) -> PodplayClient:
    global _podplay_clients
    code = client_code.lower()
    if code not in _podplay_clients:
        env_var = f"PODPLAY_API_KEY_{code.upper()}"
        api_key = os.getenv(env_var) or os.getenv("PODPLAY_API_KEY")
        if not api_key:
            raise RuntimeError(
                f"Podplay API key is not configured for client '{client_code}'. "
                f"Set {env_var} or PODPLAY_API_KEY."
            )
        _podplay_clients[code] = PodplayClient(api_key)
    return _podplay_clients[code]


def _get_podplay_client_codes() -> list[str]:
    raw_codes = os.getenv("PODPLAY_CLIENT_CODES", "gotham")
    codes = [c.strip().lower() for c in raw_codes.split(",") if c.strip()]
    if not codes:
        raise RuntimeError(
            "PODPLAY_CLIENT_CODES is empty; configure at least one client code."
        )
    return codes


def _resolve_watermark(
    source_name: str, fallback_days: int = DEFAULT_LOOKBACK_DAYS
) -> datetime:
    record = pg_client.get_elt_watermark(source_name)
    candidate = None

    if record:
        last_loaded_at, _ = record
        candidate = last_loaded_at

    if not candidate:
        candidate = datetime.now(timezone.utc) - timedelta(days=fallback_days)

    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)

    return candidate.astimezone(timezone.utc)


def refresh_courtreserve_members():
    for client_code in _get_courtreserve_client_codes():
        client = _get_courtreserve_client(client_code)
        sample_size = _get_sample_size()
        max_results = sample_size
        watermark_key = f"{EltWatermarks.MEMBERS}__{client_code}"
        start = _resolve_watermark(watermark_key)
        if sample_size:
            recent_start = datetime.now(timezone.utc) - timedelta(days=7)
            start = max(start, recent_start)

        page_size = max_results or 1000
        page_size = max(1, min(page_size, 1000))

        raw_members = client.get_members_since(
            start=start,
            record_window_days=21 if not sample_size else 7,
            page_size=page_size,
            max_results=max_results,
        )
        print(f"Pulled {len(raw_members)} CourtReserve members for {client_code}")

        print(raw_members)

        normalized_members = [
            map_cr_member(m, facility_code=client_code) for m in raw_members
        ]

        if max_results:
            normalized_members = normalized_members[:max_results]

        if not normalized_members:
            print(f"No CourtReserve members returned for {client_code}")
            continue

        pg_client.replace_members_for_client(client_code, normalized_members)
        pg_client.update_elt_watermark(watermark_key)
        print(
            f"Inserted/updated {len(normalized_members)} CourtReserve members for {client_code}"
        )


def refresh_courtreserve_reservations():
    for client_code in _get_courtreserve_client_codes():
        client = _get_courtreserve_client(client_code)
        watermark_key = f"{EltWatermarks.RESERVATIONS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)

        reservations = client.get_reservations(watermark)
        print(reservations)
        normalized_reservations = normalize_cr_reservations(
            reservations, facility_code=client_code
        )

        cancelled_reservations_by_client: dict[str, set[str]] = {}
        for record in normalized_reservations:
            reservation_id = record.get("reservation_id")
            cancelled_at = record.get("reservation_cancelled_at")
            if reservation_id and cancelled_at:
                key_client = record.get("client_code", client_code).lower()
                cancelled_reservations_by_client.setdefault(key_client, set()).add(
                    reservation_id
                )

        for (
            cancelled_client,
            reservation_ids,
        ) in cancelled_reservations_by_client.items():
            pg_client.delete_reservations_for_ids(
                cancelled_client, sorted(reservation_ids)
            )

        if cancelled_reservations_by_client:
            normalized_reservations = [
                record
                for record in normalized_reservations
                if not (
                    record.get("reservation_id")
                    and record.get("client_code", client_code).lower()
                    in cancelled_reservations_by_client
                    and record.get("reservation_id")
                    in cancelled_reservations_by_client[
                        record.get("client_code", client_code).lower()
                    ]
                )
            ]

        if not normalized_reservations:
            print(f"No reservations found for {client_code}")
            pg_client.update_elt_watermark(watermark_key)
            continue

        pg_client.insert_reservations(
            normalized_reservations, Tables.RESERVATIONS_RAW_STG
        )
        pg_client.clean_stg_records_and_insert_prod(
            watermark,
            watermark_key,
            Tables.RESERVATIONS_RAW_STG,
            Tables.RESERVATIONS_RAW,
        )


def refresh_courtreserve_reservation_cancellations():
    for client_code in _get_courtreserve_client_codes():
        client = _get_courtreserve_client(client_code)
        watermark_key = f"{EltWatermarks.RESERVATION_CANCELLATIONS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)

        reservation_cancellations = client.get_reservation_cancellations(watermark)
        normalized_reservation_cancellations = normalize_cr_cancellations(
            reservation_cancellations, facility_code=client_code
        )

        if not normalized_reservation_cancellations:
            print(f"No reservation cancellations found for {client_code}")
            pg_client.update_elt_watermark(watermark_key)
            continue

        pg_client.insert_reservation_cancellations(
            normalized_reservation_cancellations,
            Tables.RESERVATION_CANCELLATIONS_RAW_STG,
        )
        pg_client.clean_stg_records_and_insert_prod(
            watermark,
            watermark_key,
            Tables.RESERVATION_CANCELLATIONS_RAW_STG,
            Tables.RESERVATION_CANCELLATIONS_RAW,
        )


def refresh_podplay_reservations():
    for client_code in _get_podplay_client_codes():
        client = _get_podplay_client(client_code)
        watermark_key = f"{EltWatermarks.RESERVATIONS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)

        sample_size = _get_sample_size()
        max_results = sample_size
        page_size = max_results or 100
        page_size = max(1, min(page_size, 500))

        events = client.get_reservations(
            start_time=watermark,
            page_size=page_size,
            max_results=max_results,
            expand=[
                "items._links.reservations",
                "items._links.bookedBy",
                "items._links.invitations",
                "items._links.waitlist",
            ],
        )

        print(events[0], events[1], events[2])
        print(f"Pulled {len(events)} Podplay events for {client_code}")
        normalized_reservations = normalize_podplay_reservations(
            events, facility_code=client_code
        )

        if max_results:
            normalized_reservations = normalized_reservations[:max_results]
        if sample_size:
            pg_client.delete_reservations_for_client(client_code)

        if not normalized_reservations:
            print(f"No new Podplay reservations found for {client_code}")
            pg_client.update_elt_watermark(watermark_key)
            continue

        pg_client.insert_reservations(
            normalized_reservations, Tables.RESERVATIONS_RAW_STG
        )
        pg_client.clean_stg_records_and_insert_prod(
            watermark,
            watermark_key,
            Tables.RESERVATIONS_RAW_STG,
            Tables.RESERVATIONS_RAW,
        )


def refresh_podplay_members():
    for client_code in _get_podplay_client_codes():
        client = _get_podplay_client(client_code)
        watermark_key = f"{EltWatermarks.MEMBERS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)

        sample_size = _get_sample_size()
        max_results = sample_size
        page_size = max_results or 100
        page_size = max(1, min(page_size, 100))

        users = client.get_users(
            page_size=page_size,
            max_results=max_results,
            expand=["items._links.phoneNumber", "items._links.profile"],
            member_since_min=watermark,
            member_since_max=datetime.now(timezone.utc),
        )
        print(f"Pulled {len(users)} Podplay members for {client_code}")

        normalized_members = normalize_podplay_members(users, facility_code=client_code)

        if max_results:
            normalized_members = normalized_members[:max_results]

        if not normalized_members:
            print(f"No Podplay members returned for {client_code}")
            continue

        pg_client.replace_members_for_client(client_code, normalized_members)
        pg_client.update_elt_watermark(watermark_key)
        print(
            f"Inserted/updated {len(normalized_members)} Podplay members for {client_code}"
        )


def _run(option: str) -> None:
    if option == "courtreserve_reservations":
        refresh_courtreserve_reservations()
        refresh_courtreserve_reservation_cancellations()
    elif option == "courtreserve_members":
        refresh_courtreserve_members()
    elif option == "podplay_members":
        refresh_podplay_members()
    elif option == "podplay_reservations":
        refresh_podplay_reservations()
    elif option == "all":
        refresh_courtreserve_reservations()
        refresh_courtreserve_reservation_cancellations()
        refresh_podplay_members()
        refresh_podplay_reservations()
    else:
        raise SystemExit(f"Unknown option: {option}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m ingestion.main <option>")

    _run(sys.argv[1])
