from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set

from ingestion.utils.datetime import parse_iso_datetime


def _extract_booker(reservation: Dict) -> Dict:
    direct = reservation.get("bookedBy") or {}
    embedded = reservation.get("_embedded", {}).get("bookedBy") or {}

    return {
        "id": direct.get("id") or embedded.get("id") or reservation.get("bookedById"),
        "first_name": direct.get("firstName") or embedded.get("firstName"),
        "last_name": direct.get("lastName") or embedded.get("lastName"),
        "email": direct.get("email") or embedded.get("email"),
        "phone": direct.get("phoneNumber") or embedded.get("phoneNumber"),
        "display_name": direct.get("displayName")
        or embedded.get("displayName")
        or direct.get("fullName")
        or embedded.get("fullName"),
    }


def _combine_name(first: str | None, last: str | None, display: str | None) -> str | None:
    if first or last:
        return " ".join(filter(None, [first, last]))
    return display


def _normalize_member_id(candidate: Optional[str]) -> Optional[str]:
    if candidate is None:
        return None
    candidate_str = str(candidate).strip()
    return candidate_str or None


def _gather_participants(event: Dict, reservation: Dict) -> List[str]:
    invitations = (event.get("invitations") or {}).get("items") or []
    skip_statuses = {"CANCELLED", "DECLINED"}

    participants: List[str] = []
    seen: Set[str] = set()

    for invitation in invitations:
        status = (invitation.get("status") or "").upper()
        if status in skip_statuses:
            continue

        profile = invitation.get("inviteeProfile") or {}
        invitee = invitation.get("invitee") or {}

        raw_identifier = (
            profile.get("id")
            or invitee.get("email")
            or invitee.get("phoneNumber")
        )
        member_id = _normalize_member_id(raw_identifier)

        if member_id is None or member_id in seen:
            continue

        participants.append(member_id)
        seen.add(member_id)

    # Ensure the booking host is included even if invitations were missing.
    booker = _extract_booker(reservation)
    booker_identifier = (
        booker.get("id") or booker.get("email") or booker.get("phone")
    )
    host_member_id = _normalize_member_id(booker_identifier)

    if host_member_id is not None and host_member_id not in seen:
        participants.append(host_member_id)
        seen.add(host_member_id)

    return participants


def normalize_event_reservations(
    events: Iterable[Dict], facility_code: str = "podplay"
) -> List[Dict]:
    facility_code = facility_code.lower()
    rows: List[Dict] = []
    input_events = 0
    skipped_non_regular = 0
    total_reservations = 0

    for event in events:
        input_events += 1
        if (event.get("type") or "").upper() != "REGULAR":
            skipped_non_regular += 1
            continue

        event_id = event.get("id")

        event_start = parse_iso_datetime(event.get("startTime"))
        event_end = parse_iso_datetime(event.get("endTime"))

        reservations = (event.get("reservations") or {}).get("items") or []
        total_reservations += len(reservations)

        for reservation in reservations:
            raw_reservation_id = reservation.get("id") or reservation.get("code")
            reservation_id = raw_reservation_id if raw_reservation_id is not None else None

            res_start = parse_iso_datetime(reservation.get("startTime")) or event_start
            res_end = parse_iso_datetime(reservation.get("endTime")) or event_end
            res_created = parse_iso_datetime(reservation.get("createdAt"))
            res_updated = parse_iso_datetime(reservation.get("updatedAt")) or res_created
            res_cancelled = parse_iso_datetime(reservation.get("cancelledAt"))

            participants = _gather_participants(event, reservation)

            for member_id in participants:
                rows.append(
                    {
                        "client_code": facility_code,
                        "reservation_id": reservation_id,
                        "event_id": event_id,
                        "member_id": member_id,
                        "reservation_created_at": res_created,
                        "reservation_updated_at": res_updated,
                        "reservation_start_at": res_start,
                        "reservation_end_at": res_end,
                        "reservation_cancelled_at": res_cancelled,
                    }
                )

    print(
        f"[NORMALIZATION] Input events: {input_events} | "
        f"Skipped non-REGULAR: {skipped_non_regular} | "
        f"Total reservations: {total_reservations} | "
        f"Normalized reservation rows: {len(rows)}"
    )
    return rows

