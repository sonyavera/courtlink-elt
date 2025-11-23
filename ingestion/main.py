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
from ingestion.events.podplay_events import normalize_podplay_events
from ingestion.events.courtreserve_events import normalize_courtreserve_events
from ingestion.events.podplay_sessions import normalize_podplay_sessions

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
        code_upper = code.upper()
        username = os.getenv(f"{code_upper}_USERNAME")
        password = os.getenv(f"{code_upper}_PASSWORD")
        if not username or not password:
            raise RuntimeError(
                f"CourtReserve credentials are not configured for client '{client_code}'. "
                f"Set {code_upper}_USERNAME and {code_upper}_PASSWORD."
            )
        _courtreserve_clients[code] = CourtReserveClient(username, password)
    return _courtreserve_clients[code]


def _get_courtreserve_client_codes() -> list[str]:
    """Get CourtReserve client codes from database."""
    # Try environment variable first (for backward compatibility)
    raw_codes = os.getenv("CR_CLIENT_CODES")
    if raw_codes:
        codes = [c.strip().lower() for c in raw_codes.split(",") if c.strip()]
        if codes:
            return codes

    # Query database for customer organizations
    if not pg_schema:
        raise RuntimeError("PG_SCHEMA environment variable must be set")

    import psycopg2

    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()

    query = f"""
    SELECT client_code
    FROM "{pg_schema}".organizations
    WHERE is_customer = true AND source_system_code = 'courtreserve'
    ORDER BY client_code
    """

    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    codes = [row[0].strip().lower() for row in rows if row[0]]
    if not codes:
        raise RuntimeError("No CourtReserve client codes found in organizations table")
    return codes


def _get_podplay_client(client_code: str) -> PodplayClient:
    global _podplay_clients
    code = client_code.lower()
    if code not in _podplay_clients:
        code_upper = code.upper()
        api_key = os.getenv(f"{code_upper}_API_KEY")
        if not api_key:
            raise RuntimeError(
                f"Podplay API key is not configured for client '{client_code}'. "
                f"Set {code_upper}_API_KEY."
            )
        _podplay_clients[code] = PodplayClient(api_key)
    return _podplay_clients[code]


def _get_podplay_client_codes() -> list[str]:
    """Get Podplay client codes from database."""
    # Try environment variable first (for backward compatibility)
    raw_codes = os.getenv("PODPLAY_CLIENT_CODES")
    if raw_codes:
        codes = [c.strip().lower() for c in raw_codes.split(",") if c.strip()]
        if codes:
            return codes

    # Query database for customer organizations
    if not pg_schema:
        raise RuntimeError("PG_SCHEMA environment variable must be set")

    import psycopg2

    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()

    query = f"""
    SELECT client_code
    FROM "{pg_schema}".organizations
    WHERE is_customer = true AND source_system_code = 'podplay'
    ORDER BY client_code
    """

    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    codes = [row[0].strip().lower() for row in rows if row[0]]
    if not codes:
        raise RuntimeError("No Podplay client codes found in organizations table")
    return codes


def _get_podplay_clients_with_pod_ids() -> list[tuple[str, Optional[str]]]:
    """Get Podplay client codes and pod IDs from database.

    Returns:
        List of tuples (client_code, podplay_pod_id)
    """
    # Query database for customer organizations
    if not pg_schema:
        raise RuntimeError("PG_SCHEMA environment variable must be set")

    import psycopg2

    conn = psycopg2.connect(pg_dsn)
    cur = conn.cursor()

    query = f"""
    SELECT client_code, podplay_pod_id
    FROM "{pg_schema}".organizations
    WHERE is_customer = true AND source_system_code = 'podplay'
    ORDER BY client_code
    """

    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    result = [
        (row[0].strip().lower(), row[1] if row[1] else None) for row in rows if row[0]
    ]
    return result


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

        print(f"\n[PODPLAY RESERVATIONS] Starting API calls to get events...")
        print(
            f"[PODPLAY RESERVATIONS] Filtering for type=REGULAR (court reservations only)"
        )
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
            event_type="REGULAR",  # Only get court reservations
        )
        print(
            f"\n[PODPLAY RESERVATIONS] API calls complete: {len(events)} total events retrieved"
        )

        # Log first 3 API results
        print(f"\n[PODPLAY RESERVATIONS] First 3 API results:")
        for i, event in enumerate(events[:3], 1):
            print(f"  Result {i}: {event}")
        if len(events) > 3:
            print(f"  ... and {len(events) - 3} more results")

        print(f"\n[PODPLAY RESERVATIONS] Normalizing events to reservations...")
        normalized_reservations = normalize_podplay_reservations(
            events, facility_code=client_code
        )
        print(
            f"[PODPLAY RESERVATIONS] Normalization complete: {len(events)} events → "
            f"{len(normalized_reservations)} reservations"
        )

        if max_results:
            original_count = len(normalized_reservations)
            normalized_reservations = normalized_reservations[:max_results]
            if original_count > max_results:
                print(
                    f"[PODPLAY RESERVATIONS] Limited from {original_count} to {max_results} "
                    f"due to max_results"
                )

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


def refresh_podplay_events():
    """Refresh Podplay events for all participating facilities."""
    print("=" * 80)
    print("[PODPLAY EVENTS] Starting Podplay events ingestion")
    print("=" * 80)

    clients_with_pod_ids = _get_podplay_clients_with_pod_ids()
    if not clients_with_pod_ids:
        print("[PODPLAY EVENTS] No Podplay clients found, skipping")
        return

    # Calculate date range: now to 7 days from now
    now = datetime.now(timezone.utc)
    end_time = now + timedelta(days=7)

    all_events = []
    all_raw_events = []  # Store raw API responses

    for client_code, pod_id in clients_with_pod_ids:
        print(f"\n[PODPLAY EVENTS] Processing {client_code} (pod_id: {pod_id})...")

        try:
            client = _get_podplay_client(client_code)

            # Get events with all types
            events = client.get_events(
                start_time=now,
                end_time=end_time,
                event_types=["REGULAR", "CLASS", "EVENT"],
                pod_id=pod_id,
            )

            print(f"[PODPLAY EVENTS] Retrieved {len(events)} events for {client_code}")

            # Save raw API response for inspection
            all_raw_events.extend(events)

            # Normalize events
            normalized = normalize_podplay_events(events, client_code)
            all_events.extend(normalized)

            print(
                f"[PODPLAY EVENTS] Normalized {len(normalized)} events for {client_code}"
            )

        except Exception as e:
            print(
                f"[PODPLAY EVENTS] Error processing {client_code}: {e}", file=sys.stderr
            )
            continue

    # Save raw API responses to JSON file for inspection
    import json

    raw_output_file = "podplay_events_raw_api_response.json"
    with open(raw_output_file, "w") as f:
        json.dump(all_raw_events, f, indent=2, default=str)
    print(
        f"\n[PODPLAY EVENTS] Saved {len(all_raw_events)} raw API events to {raw_output_file}"
    )

    # Save normalized events to JSON file for inspection
    output_file = "podplay_events_output.json"
    with open(output_file, "w") as f:
        json.dump(all_events, f, indent=2, default=str)
    print(
        f"[PODPLAY EVENTS] Saved {len(all_events)} normalized events to {output_file}"
    )

    # Insert into database
    if all_events:
        print(f"\n[PODPLAY EVENTS] Inserting {len(all_events)} events into database...")
        pg_client.insert_events(all_events)
        print(f"[PODPLAY EVENTS] ✓ Complete: {len(all_events)} events inserted")
    else:
        print("[PODPLAY EVENTS] No events to insert")

    print("[PODPLAY EVENTS] All clients processed")
    print("=" * 80)


def refresh_courtreserve_events():
    """Refresh CourtReserve events for all participating facilities."""
    print("=" * 80)
    print("[COURTRESERVE EVENTS] Starting CourtReserve events ingestion")
    print("=" * 80)

    client_codes = _get_courtreserve_client_codes()
    if not client_codes:
        print("[COURTRESERVE EVENTS] No CourtReserve clients found, skipping")
        return

    # Calculate date range: now to 7 days from now
    now = datetime.now(timezone.utc)
    end_date = now + timedelta(days=7)

    all_events = []
    all_raw_events = []  # Store raw API responses

    for client_code in client_codes:
        print(f"\n[COURTRESERVE EVENTS] Processing {client_code}...")

        try:
            client = _get_courtreserve_client(client_code)

            # Get events
            events = client.get_events(
                start_date=now,
                end_date=end_date,
            )

            print(
                f"[COURTRESERVE EVENTS] Retrieved {len(events)} events for {client_code}"
            )

            # Save raw API response for inspection
            all_raw_events.extend(events)

            # Normalize events
            normalized = normalize_courtreserve_events(events, client_code)
            all_events.extend(normalized)

            print(
                f"[COURTRESERVE EVENTS] Normalized {len(normalized)} events for {client_code}"
            )

        except Exception as e:
            print(
                f"[COURTRESERVE EVENTS] Error processing {client_code}: {e}",
                file=sys.stderr,
            )
            continue

    # Save raw API responses to JSON file for inspection
    import json

    raw_output_file = "courtreserve_events_raw_api_response.json"
    with open(raw_output_file, "w") as f:
        json.dump(all_raw_events, f, indent=2, default=str)
    print(
        f"\n[COURTRESERVE EVENTS] Saved {len(all_raw_events)} raw API events to {raw_output_file}"
    )

    # Save normalized events to JSON file for inspection
    output_file = "courtreserve_events_output.json"
    with open(output_file, "w") as f:
        json.dump(all_events, f, indent=2, default=str)
    print(
        f"[COURTRESERVE EVENTS] Saved {len(all_events)} normalized events to {output_file}"
    )

    # Check for duplicates in normalized events
    # Primary key is now (client_code, source_system, event_id, event_start_time)
    from collections import Counter

    event_keys = [
        (e["client_code"], e["source_system"], e["event_id"], e.get("event_start_time"))
        for e in all_events
        if e.get("event_start_time")  # Only check events with start_time
    ]
    duplicate_keys = [key for key, count in Counter(event_keys).items() if count > 1]
    if duplicate_keys:
        print(
            f"\n[COURTRESERVE EVENTS] WARNING: Found {len(duplicate_keys)} duplicate event keys:"
        )
        for key in duplicate_keys[:10]:  # Show first 10
            print(f"  - {key}")
        if len(duplicate_keys) > 10:
            print(f"  ... and {len(duplicate_keys) - 10} more")

        # Save duplicate analysis
        duplicate_analysis = []
        for key in duplicate_keys:
            duplicates = [
                e
                for e in all_events
                if (
                    e["client_code"],
                    e["source_system"],
                    e["event_id"],
                    e.get("event_start_time"),
                )
                == key
            ]
            duplicate_analysis.append(
                {"key": key, "count": len(duplicates), "events": duplicates}
            )

        dup_file = "courtreserve_events_duplicates.json"
        with open(dup_file, "w") as f:
            json.dump(duplicate_analysis, f, indent=2, default=str)
        print(f"[COURTRESERVE EVENTS] Saved duplicate analysis to {dup_file}")

    # Insert into database
    if all_events:
        print(
            f"\n[COURTRESERVE EVENTS] Inserting {len(all_events)} events into database..."
        )
        pg_client.insert_events(all_events)
        print(f"[COURTRESERVE EVENTS] ✓ Complete: {len(all_events)} events inserted")
    else:
        print("[COURTRESERVE EVENTS] No events to insert")

    print("[COURTRESERVE EVENTS] All clients processed")
    print("=" * 80)


def refresh_podplay_court_availability():
    """Refresh Podplay court availability for all participating facilities."""
    print("=" * 80)
    print("[PODPLAY COURT AVAILABILITY] Starting Podplay court availability ingestion")
    print("=" * 80)

    clients_with_pod_ids = _get_podplay_clients_with_pod_ids()
    if not clients_with_pod_ids:
        print("[PODPLAY COURT AVAILABILITY] No Podplay clients found, skipping")
        return

    # Calculate date range: now to 7 days from now
    now = datetime.now(timezone.utc)
    end_time = now + timedelta(days=7)

    print(
        f"[PODPLAY COURT AVAILABILITY] Date range: {now.isoformat()} to {end_time.isoformat()}"
    )

    all_sessions = []
    all_raw_sessions = []  # Store raw API responses

    for client_code, pod_id in clients_with_pod_ids:
        print(
            f"\n[PODPLAY COURT AVAILABILITY] Processing {client_code} (pod_id: {pod_id})..."
        )

        try:
            client = _get_podplay_client(client_code)

            # Get sessions
            sessions = client.get_sessions(
                start_time=now,
                end_time=end_time,
                pod_id=pod_id,
            )

            print(
                f"[PODPLAY COURT AVAILABILITY] Retrieved {len(sessions)} sessions for {client_code}"
            )

            # Save raw API response for inspection
            all_raw_sessions.extend(sessions)

            # Normalize sessions (pass end_time for date filtering)
            normalized = normalize_podplay_sessions(sessions, client_code, end_time)
            all_sessions.extend(normalized)

            print(
                f"[PODPLAY COURT AVAILABILITY] Normalized {len(normalized)} sessions for {client_code}"
            )

        except Exception as e:
            print(
                f"[PODPLAY COURT AVAILABILITY] Error processing {client_code}: {e}",
                file=sys.stderr,
            )
            continue

    # Save raw API responses to JSON file for inspection
    import json

    raw_output_file = "podplay_sessions_raw_api_response.json"
    with open(raw_output_file, "w") as f:
        json.dump(all_raw_sessions, f, indent=2, default=str)
    print(
        f"\n[PODPLAY COURT AVAILABILITY] Saved {len(all_raw_sessions)} raw API sessions to {raw_output_file}"
    )

    # Save normalized sessions to JSON file for inspection
    output_file = "podplay_sessions_output.json"
    with open(output_file, "w") as f:
        json.dump(all_sessions, f, indent=2, default=str)
    print(
        f"[PODPLAY COURT AVAILABILITY] Saved {len(all_sessions)} normalized sessions to {output_file}"
    )

    # Replace all court availabilities in database (safe table swap)
    if all_sessions:
        print(
            f"\n[PODPLAY COURT AVAILABILITY] Replacing all court availabilities in database..."
        )
        pg_client.replace_court_availabilities(all_sessions)
        print(
            f"[PODPLAY COURT AVAILABILITY] ✓ Complete: {len(all_sessions)} sessions inserted"
        )
    else:
        print("[PODPLAY COURT AVAILABILITY] No sessions to insert")

    print("[PODPLAY COURT AVAILABILITY] All clients processed")
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
    elif option == "podplay_events":
        refresh_podplay_events()
    elif option == "courtreserve_events":
        refresh_courtreserve_events()
    elif option == "podplay_court_availability":
        refresh_podplay_court_availability()
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
