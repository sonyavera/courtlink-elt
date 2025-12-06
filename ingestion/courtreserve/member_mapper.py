from datetime import date, datetime
from typing import Optional

from ingestion.utils.normalize import normalize_email, normalize_phone_number


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

    # Get phone number from either field
    raw_phone = member.get("PhoneNumber") or member.get("MobilePhone")
    # Normalize phone number and email
    phone_number = normalize_phone_number(raw_phone)
    email = normalize_email(member.get("Email"))

    # Extract membership type from CourtReserve API
    # Check if member is Staff first, then use MembershipTypeName
    membership_type_name = None
    if member.get("Staff") or member.get("IsStaff"):
        membership_type_name = "Staff"
    else:
        membership_type_name = member.get("MembershipTypeName") or "Non-Member Account"
    
    # is_premium_member: 1 if membership_type_name is NOT "Non-Member Account", 0 if it is
    # This means Staff, Founder, Premium, etc. are all premium (1)
    is_premium_member = 1 if membership_type_name and membership_type_name != "Non-Member Account" else 0

    # Extract MembershipStartDate from CourtReserve API
    member_since_date = parse_date(member.get("MembershipStartDate"))

    return {
        "client_code": facility_code,
        "member_id": str(member_id) if member_id is not None else None,
        "club_member_key": str(member_id) if member_id is not None else None,
        "first_name": member.get("FirstName"),
        "last_name": member.get("LastName"),
        "gender": member.get("Gender"),
        "date_of_birth": parse_date(member.get("DateOfBirth")),
        "email": email,
        "phone_number": phone_number,
        "membership_type_name": membership_type_name,
        "is_premium_member": is_premium_member,
        "member_since": member_since_date,
    }
