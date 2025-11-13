from __future__ import annotations

from typing import Dict, Iterable, List, Optional

from ingestion.utils.datetime import format_date, parse_iso_datetime
from ingestion.utils.normalize import normalize_email, normalize_phone_number


def _resolve_primary_membership(user: Dict) -> Dict:
    memberships = (user.get("memberships") or {}).get("items") or []
    if not memberships:
        return {}

    # sort newest first using created timestamp when available
    def sort_key(item: Dict) -> tuple:
        created = parse_iso_datetime(
            item.get("createdAt")
            or item.get("startedAt")
            or item.get("membership", {}).get("createdAt")
        )
        created_str = created.isoformat() if created else ""
        return (created_str, item.get("membership", {}).get("id"))

    return sorted(memberships, key=sort_key, reverse=True)[0]


def normalize_members(
    users: Iterable[Dict], facility_code: str = "podplay"
) -> List[Dict]:
    facility_code = facility_code.lower()
    normalized: List[Dict] = []
    input_count = 0

    for user in users:
        input_count += 1
        membership = _resolve_primary_membership(user)

        phone_number = user.get("phoneNumber")
        if isinstance(phone_number, dict):
            phone_number = (
                phone_number.get("phoneNumber")
                or phone_number.get("id")
                or phone_number.get("value")
            )
        
        # Normalize phone number and email
        phone_number = normalize_phone_number(phone_number)
        email = normalize_email(user.get("email"))

        user_id = user.get("id")

        birthday_raw = user.get("birthday")
        birthday_date = None
        if birthday_raw:
            parsed_birthday = parse_iso_datetime(birthday_raw)
            if parsed_birthday:
                birthday_date = parsed_birthday.date()

        normalized.append(
            {
                "client_code": facility_code,
                "member_id": user_id,
                "club_member_key": user_id,
                "first_name": user.get("firstName"),
                "last_name": user.get("lastName"),
                "gender": user.get("gender"),
                "date_of_birth": birthday_date,
                "email": email,
                "phone_number": phone_number,
            }
        )

    print(
        f"[NORMALIZATION] Input users: {input_count} | "
        f"Normalized members: {len(normalized)}"
    )
    return normalized
