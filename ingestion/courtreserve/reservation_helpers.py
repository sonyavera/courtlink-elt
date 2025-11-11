from .date_helpers import parse_event_time, parse_utc_time


def map_members_on_reservation(
    players: list[dict], res_metadata: dict, facility_code: str
) -> list[dict]:
    if players is None:
        return []
    rows = []
    for player in players:
        member_id_raw = player.get("OrganizationMemberId")
        member_metadata = {
            "member_id": str(member_id_raw) if member_id_raw is not None else None,
            "club_member_key": (
                str(member_id_raw) if member_id_raw is not None else None
            ),
        }

        rows.append({**res_metadata, **member_metadata})

    return rows


def normalize_reservations(
    reservations: list[dict], facility_code: str = "pklyn"
) -> list[dict]:
    facility_code = facility_code.lower()
    all_reservations = []
    for res in reservations:
        start_dt = parse_event_time(res.get("StartTime"))
        end_dt = parse_event_time(res.get("EndTime"))
        created_dt = parse_utc_time(res.get("CreatedOnUtc"))
        updated_dt = parse_utc_time(res.get("UpdatedOnUtc"))
        cancelled_raw = res.get("CancelledOnUtc") or res.get("CancelledOn")
        cancelled_dt = parse_utc_time(cancelled_raw) if cancelled_raw else None
        members = res.get("Players")

        reservation_type_id = res.get("ReservationTypeId")
        reservation_id = res.get("Id")

        res_metadata = {
            "client_code": facility_code,
            "reservation_id": (
                str(reservation_id) if reservation_id is not None else None
            ),
            "event_id": (
                str(reservation_type_id) if reservation_type_id is not None else None
            ),
            "reservation_created_at": created_dt,
            "reservation_updated_at": updated_dt,
            "reservation_start_at": start_dt,
            "reservation_end_at": end_dt,
            "reservation_cancelled_at": cancelled_dt,
        }

        print("RES_METADATA", res_metadata)

        rows = map_members_on_reservation(members, res_metadata, facility_code)
        all_reservations.extend(rows)

    return all_reservations
