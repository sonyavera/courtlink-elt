from __future__ import annotations

import re
from typing import Optional


def normalize_phone_number(phone: Optional[str]) -> Optional[str]:
    """
    Normalize phone numbers to consistent format.
    
    Rules:
    - US numbers (10 digits or starting with 1): Normalize to +1XXXXXXXXXX
    - International numbers (starting with + but not +1): Keep as-is
    - Invalid/empty: Return None
    
    Examples:
    - "+12345678901" -> "+12345678901"
    - "12345678901" -> "+12345678901"
    - "2345678901" -> "+12345678901"
    - "+442071234567" -> "+442071234567" (international, keep as-is)
    - None or "" -> None
    """
    if not phone:
        return None
    
    # Convert to string and strip whitespace
    phone_str = str(phone).strip()
    if not phone_str:
        return None
    
    # Remove common formatting characters (spaces, dashes, parentheses, dots)
    # But keep + sign
    phone_clean = re.sub(r'[\s\-\(\)\.]', '', phone_str)
    
    # If it's already in +1XXXXXXXXXX format, return as-is
    if phone_clean.startswith('+1') and len(phone_clean) == 12:
        return phone_clean
    
    # If it starts with + but not +1, it's international - keep as-is
    if phone_clean.startswith('+') and not phone_clean.startswith('+1'):
        return phone_clean
    
    # Extract only digits
    digits_only = re.sub(r'\D', '', phone_clean)
    
    if not digits_only:
        return None
    
    # Handle US numbers
    # If 11 digits starting with 1, it's +1XXXXXXXXXX
    if len(digits_only) == 11 and digits_only.startswith('1'):
        return f"+{digits_only}"
    
    # If 10 digits, it's a US number without country code
    if len(digits_only) == 10:
        return f"+1{digits_only}"
    
    # If it starts with 1 but has more than 11 digits, might be malformed
    # Try to extract first 11 digits
    if digits_only.startswith('1') and len(digits_only) > 11:
        return f"+{digits_only[:11]}"
    
    # If it's 11 digits but doesn't start with 1, might be international
    # For now, assume it's a US number with leading 1
    if len(digits_only) == 11:
        return f"+{digits_only}"
    
    # If it's less than 10 digits, it's probably invalid
    if len(digits_only) < 10:
        return None
    
    # For anything else, try to format as US number if it looks like it
    # (starts with valid area code, etc.)
    if len(digits_only) >= 10:
        # Take last 10 digits and add +1
        last_10 = digits_only[-10:]
        return f"+1{last_10}"
    
    # If we can't figure it out, return None
    return None


def normalize_email(email: Optional[str]) -> Optional[str]:
    """
    Normalize email addresses to consistent format.
    
    Rules:
    - Lowercase
    - Trim whitespace
    - Return None if empty/invalid
    
    Examples:
    - "John.Doe@Example.COM" -> "john.doe@example.com"
    - "  user@email.com  " -> "user@email.com"
    - None or "" -> None
    """
    if not email:
        return None
    
    email_str = str(email).strip().lower()
    
    if not email_str:
        return None
    
    # Basic email validation (has @ and at least one character before and after)
    if '@' not in email_str or len(email_str.split('@')) != 2:
        return None
    
    local, domain = email_str.split('@')
    if not local or not domain:
        return None
    
    return email_str

