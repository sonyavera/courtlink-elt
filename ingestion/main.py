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
    print("=" * 80)
    print("[COURTRESERVE MEMBERS] Starting CourtReserve members ingestion")
    print("=" * 80)

    for client_code in _get_courtreserve_client_codes():
        print(f"\n[COURTRESERVE MEMBERS] Processing client: {client_code}")
        print("-" * 80)

        client = _get_courtreserve_client(client_code)
        sample_size = _get_sample_size()
        max_results = sample_size
        watermark_key = f"{EltWatermarks.MEMBERS}__{client_code}"
        start = _resolve_watermark(watermark_key)
        print(f"[COURTRESERVE MEMBERS] Watermark for {client_code}: {start}")

        if sample_size:
            recent_start = datetime.now(timezone.utc) - timedelta(days=7)
            start = max(start, recent_start)
            print(
                f"[COURTRESERVE MEMBERS] Sample mode: adjusted start to {start} "
                f"(last 7 days)"
            )

        page_size = max_results or 1000
        page_size = max(1, min(page_size, 1000))
        record_window_days = 21 if not sample_size else 7

        print(
            f"[COURTRESERVE MEMBERS] Configuration: page_size={page_size}, "
            f"max_results={max_results}, record_window_days={record_window_days}, "
            f"sample_size={sample_size}"
        )

        print(f"\n[COURTRESERVE MEMBERS] Starting API calls to get members...")
        raw_members = client.get_members_since(
            start=start,
            record_window_days=record_window_days,
            page_size=page_size,
            max_results=max_results,
        )
        print(
            f"\n[COURTRESERVE MEMBERS] API calls complete: {len(raw_members)} total members retrieved"
        )

        print(f"\n[COURTRESERVE MEMBERS] Normalizing members...")
        normalized_members = [
            map_cr_member(m, facility_code=client_code) for m in raw_members
        ]
        print(
            f"[COURTRESERVE MEMBERS] Normalization complete: {len(raw_members)} raw → "
            f"{len(normalized_members)} normalized"
        )

        if max_results:
            original_count = len(normalized_members)
            normalized_members = normalized_members[:max_results]
            if original_count > max_results:
                print(
                    f"[COURTRESERVE MEMBERS] Limited from {original_count} to {max_results} "
                    f"due to max_results"
                )

        if not normalized_members:
            print(
                f"[COURTRESERVE MEMBERS] No normalized members for {client_code}, skipping"
            )
            continue

        print(f"\n[COURTRESERVE MEMBERS] Replacing members in database...")
        pg_client.replace_members_for_client(client_code, normalized_members)

        print(f"\n[COURTRESERVE MEMBERS] Updating watermark...")
        pg_client.update_elt_watermark(watermark_key)

        print(
            f"\n[COURTRESERVE MEMBERS] ✓ Complete for {client_code}: {len(normalized_members)} members processed"
        )
        print("-" * 80)

    print("\n" + "=" * 80)
    print("[COURTRESERVE MEMBERS] All clients processed")
    print("=" * 80)


def refresh_courtreserve_reservations():
    for client_code in _get_courtreserve_client_codes():
        client = _get_courtreserve_client(client_code)
        watermark_key = f"{EltWatermarks.RESERVATIONS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)

        reservations = client.get_reservations(watermark)

        # Log first 3 API results
        print(f"\n[COURTRESERVE RESERVATIONS] First 3 API results:")
        for i, res in enumerate(reservations[:3], 1):
            print(f"  Result {i}: {res}")
        if len(reservations) > 3:
            print(f"  ... and {len(reservations) - 3} more results")

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
    print("=" * 80)
    print("[PODPLAY RESERVATIONS] Starting Podplay reservations ingestion")
    print("=" * 80)

    for client_code in _get_podplay_client_codes():
        print(f"\n[PODPLAY RESERVATIONS] Processing client: {client_code}")
        print("-" * 80)

        client = _get_podplay_client(client_code)
        watermark_key = f"{EltWatermarks.RESERVATIONS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)
        print(f"[PODPLAY RESERVATIONS] Watermark for {client_code}: {watermark}")

        sample_size = _get_sample_size()
        max_results = sample_size
        page_size = max_results or 500
        page_size = max(1, min(page_size, 500))

        print(
            f"[PODPLAY RESERVATIONS] Configuration: page_size={page_size}, "
            f"max_results={max_results}, sample_size={sample_size}"
        )

        if sample_size:
            print(
                f"[PODPLAY RESERVATIONS] Sample mode: will delete existing reservations for {client_code}"
            )

        print(f"\n[PODPLAY RESERVATIONS] Starting batched API calls...")
        print(
            f"[PODPLAY RESERVATIONS] Filtering for type=REGULAR (court reservations only)"
        )
        print(
            f"[PODPLAY RESERVATIONS] Will pull in 10-day windows until 3 weeks from today"
        )

        # Stop 3 weeks from today (21 days ahead)
        max_end_time = datetime.now(timezone.utc) + timedelta(days=21)
        window_days = 10
        current_start = watermark
        all_normalized_reservations = []
        window_num = 0

        while current_start < max_end_time:
            window_num += 1
            current_end = min(current_start + timedelta(days=window_days), max_end_time)

            print(
                f"\n[PODPLAY RESERVATIONS] Window {window_num}: "
                f"{current_start.date()} to {current_end.date()}"
            )

            events = client.get_reservations(
                start_time=current_start,
                end_time=current_end,
                page_size=page_size,
                max_results=max_results,
                expand=[
                    "items._links.reservations",
                    "items._links.bookedBy",
                    "items._links.invitations",
                    "items._links.waitlist",
                ],
                event_type="REGULAR",  # Only get court reservations
            )

            print(
                f"[PODPLAY RESERVATIONS] Window {window_num}: {len(events)} events retrieved"
            )

            if window_num == 1 and events:
                # Log first 3 API results from first window only
                print(f"\n[PODPLAY RESERVATIONS] First 3 API results (from window 1):")
                for i, event in enumerate(events[:3], 1):
                    print(f"  Result {i}: {event}")
                if len(events) > 3:
                    print(f"  ... and {len(events) - 3} more results")

            print(
                f"[PODPLAY RESERVATIONS] Normalizing events from window {window_num}..."
            )
            window_reservations = normalize_podplay_reservations(
                events, facility_code=client_code
            )
            print(
                f"[PODPLAY RESERVATIONS] Window {window_num}: {len(events)} events → "
                f"{len(window_reservations)} reservations"
            )

            all_normalized_reservations.extend(window_reservations)

            # Move to next window
            current_start = current_end

            # If we hit max_results, stop
            if max_results and len(all_normalized_reservations) >= max_results:
                print(
                    f"[PODPLAY RESERVATIONS] Reached max_results={max_results}, stopping"
                )
                all_normalized_reservations = all_normalized_reservations[:max_results]
                break

        print(
            f"\n[PODPLAY RESERVATIONS] All windows complete: "
            f"{len(all_normalized_reservations)} total reservations from {window_num} windows"
        )

        normalized_reservations = all_normalized_reservations

        if sample_size:
            print(
                f"\n[PODPLAY RESERVATIONS] Deleting existing reservations for {client_code} (sample mode)..."
            )
            pg_client.delete_reservations_for_client(client_code)

        if not normalized_reservations:
            print(
                f"[PODPLAY RESERVATIONS] No normalized reservations for {client_code}, skipping"
            )
            pg_client.update_elt_watermark(watermark_key)
            continue

        print(f"\n[PODPLAY RESERVATIONS] Inserting reservations into STG...")
        pg_client.insert_reservations(
            normalized_reservations, Tables.RESERVATIONS_RAW_STG
        )

        print(f"\n[PODPLAY RESERVATIONS] Cleaning STG and moving to PROD...")
        pg_client.clean_stg_records_and_insert_prod(
            watermark,
            watermark_key,
            Tables.RESERVATIONS_RAW_STG,
            Tables.RESERVATIONS_RAW,
        )

        print(
            f"\n[PODPLAY RESERVATIONS] ✓ Complete for {client_code}: {len(normalized_reservations)} reservations processed"
        )
        print("-" * 80)

    print("\n" + "=" * 80)
    print("[PODPLAY RESERVATIONS] All clients processed")
    print("=" * 80)


def refresh_podplay_members():
    print("=" * 80)
    print("[PODPLAY MEMBERS] Starting Podplay members ingestion")
    print("=" * 80)

    for client_code in _get_podplay_client_codes():
        print(f"\n[PODPLAY MEMBERS] Processing client: {client_code}")
        print("-" * 80)

        client = _get_podplay_client(client_code)
        watermark_key = f"{EltWatermarks.MEMBERS}__{client_code}"
        watermark = _resolve_watermark(watermark_key)
        print(f"[PODPLAY MEMBERS] Watermark for {client_code}: {watermark}")

        sample_size = _get_sample_size()
        max_results = sample_size
        # Use larger page size for full pulls to reduce API calls (14k members = ~28 calls at 500/page)
        page_size = max_results or 500
        page_size = max(1, min(page_size, 500))  # Cap at 500 to be safe
        print(
            f"[PODPLAY MEMBERS] Configuration: page_size={page_size}, "
            f"max_results={max_results}, sample_size={sample_size}"
        )
        if not max_results:
            estimated_calls = 14000 // page_size + 1
            print(
                f"[PODPLAY MEMBERS] Estimated ~{estimated_calls} API calls for ~14k members "
                f"(at {page_size} per page)"
            )

        print(f"\n[PODPLAY MEMBERS] Starting API calls to get users...")
        print(
            f"[PODPLAY MEMBERS] NOTE: Pulling ALL members (not filtering by member_since)"
        )
        users = client.get_users(
            page_size=page_size,
            max_results=max_results,
            expand=["items._links.phoneNumber", "items._links.profile"],
            # Removed member_since_min/max to get ALL members, not just recent ones
        )
        print(
            f"\n[PODPLAY MEMBERS] API calls complete: {len(users)} total users retrieved"
        )

        print(f"\n[PODPLAY MEMBERS] Normalizing users...")
        normalized_members = normalize_podplay_members(users, facility_code=client_code)

        if max_results:
            original_count = len(normalized_members)
            normalized_members = normalized_members[:max_results]
            if original_count > max_results:
                print(
                    f"[PODPLAY MEMBERS] Limited from {original_count} to {max_results} "
                    f"due to max_results"
                )

        if not normalized_members:
            print(
                f"[PODPLAY MEMBERS] No normalized members for {client_code}, skipping"
            )
            continue

        print(f"\n[PODPLAY MEMBERS] Replacing members in database...")
        pg_client.replace_members_for_client(client_code, normalized_members)

        print(f"\n[PODPLAY MEMBERS] Updating watermark...")
        pg_client.update_elt_watermark(watermark_key)

        print(
            f"\n[PODPLAY MEMBERS] ✓ Complete for {client_code}: {len(normalized_members)} members processed"
        )
        print("-" * 80)

    print("\n" + "=" * 80)
    print("[PODPLAY MEMBERS] All clients processed")
    print("=" * 80)


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
