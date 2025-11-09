from datetime import date, datetime
from typing import Optional


def parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        return None


def map_member_to_row(member: dict, facility_code: str) -> dict:
    facility_code = facility_code.lower()
    member_id = member.get("MembershipNumber")

    return {
        "client_code": facility_code,
        "member_id": str(member_id) if member_id is not None else None,
        "club_member_key": str(member_id) if member_id is not None else None,
        "first_name": member.get("FirstName"),
        "last_name": member.get("LastName"),
        "gender": member.get("Gender"),
        "date_of_birth": parse_date(member.get("DateOfBirth")),
        "email": member.get("Email"),
        "phone_number": member.get("PhoneNumber") or member.get("MobilePhone"),
    }
