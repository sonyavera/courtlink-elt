from .date_helpers import parse_event_time, parse_utc_time


def normalize_reservation_cancellations(
    reservation_cancellations: list[dict],
    facility_code: str = "pklyn",
) -> list[dict]:
    facility_code = facility_code.lower()
    source_system = "courtreserve"
    rows = []
    for cancellation in reservation_cancellations:
        print(cancellation)
        start_dt = parse_event_time(cancellation.get("StartTime"))
        end_dt = parse_event_time(cancellation.get("EndTime"))
        created_dt = parse_utc_time(cancellation.get("SignedUpOnUtc"))
        cancelled_raw = cancellation.get("CancelledOnUtc") or cancellation.get(
            "CancelledOn"
        )
        canceled_at = parse_utc_time(cancelled_raw) if cancelled_raw else None

        event_id = cancellation.get("EventId")
        reservation_id = cancellation.get("EventDateId")
        member_id_raw = cancellation.get("OrganizationMemberId")

        player_first = cancellation.get("FirstName")
        player_last = cancellation.get("LastName")
        player_name = " ".join(filter(None, [player_first, player_last]))

        reservation_metadata = {
            "client_code": facility_code,
            "source_system": source_system,
            "event_id": str(event_id) if event_id is not None else None,
            "reservation_id": (
                str(reservation_id) if reservation_id is not None else None
            ),
            "member_id": str(member_id_raw) if member_id_raw is not None else None,
            "reservation_type": cancellation.get("EventCategoryName"),
            "reservation_created_at": created_dt,
            "reservation_start_at": start_dt,
            "reservation_end_at": end_dt,
            "cancelled_on": canceled_at,
            "day_of_week": start_dt.strftime("%A") if start_dt else None,
            "is_program": True,
            "program_name": cancellation.get("EventName"),
            "player_name": player_name,
            "player_first_name": player_first,
            "player_last_name": player_last,
            "player_email": cancellation.get("Email"),
            "player_phone": cancellation.get("Phone"),
            "fee": cancellation.get("PriceToPay"),
            "is_team_event": cancellation.get("IsTeamEvent"),
            "event_category_name": cancellation.get("EventCategoryName"),
            "event_category_id": cancellation.get("EventCategoryId"),
        }
        rows.append(reservation_metadata)
    return rows
