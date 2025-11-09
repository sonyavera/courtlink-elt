from __future__ import annotations

from typing import Dict, Iterable, List

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


def normalize_event_reservations(
    events: Iterable[Dict], facility_code: str = "podplay"
) -> List[Dict]:
    facility_code = facility_code.lower()
    rows: List[Dict] = []

    for event in events:
        event_id = event.get("id")

        event_start = parse_iso_datetime(event.get("startTime"))
        event_end = parse_iso_datetime(event.get("endTime"))

        reservations = (event.get("reservations") or {}).get("items") or []

        for reservation in reservations:
            raw_reservation_id = reservation.get("id") or reservation.get("code")
            reservation_id = raw_reservation_id if raw_reservation_id is not None else None

            res_start = parse_iso_datetime(reservation.get("startTime")) or event_start
            res_end = parse_iso_datetime(reservation.get("endTime")) or event_end
            res_created = parse_iso_datetime(reservation.get("createdAt"))
            res_updated = parse_iso_datetime(reservation.get("updatedAt")) or res_created

            booker = _extract_booker(reservation)
            source_member_id = booker.get("id")
            member_id = (
                f"{facility_code}:{source_member_id}"
                if source_member_id is not None
                else None
            )

            rows.append(
                {
                    "client_code": facility_code,
                    "reservation_id": reservation_id,
                    "event_id": event_id,
                    "member_id": member_id,
                    "club_member_key": (
                        f"{facility_code}:{source_member_id}"
                        if source_member_id is not None
                        else None
                    ),
                    "reservation_created_at": res_created,
                    "reservation_updated_at": res_updated,
                    "reservation_start_at": res_start,
                    "reservation_end_at": res_end,
                }
            )

    return rows

