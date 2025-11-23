"""Normalize CourtReserve events for database storage."""

from datetime import datetime, timezone
from typing import Dict, List, Optional


def _load_event_categories(client_code: str, source_system: str = "courtreserve") -> Dict[str, str]:
    """
    Load event categories from database.
    
    Returns:
        Dictionary mapping category_id -> event_category_name
    """
    try:
        import os
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        pg_dsn = os.getenv("PG_DSN")
        schema = os.getenv("PG_SCHEMA")
        
        if not pg_dsn or not schema:
            return {}
        
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        
        query = f"""
        SELECT id, event_category_name
        FROM "{schema}".facility_event_categories
        WHERE client_code = %s AND source_system = %s
        """
        
        cur.execute(query, (client_code.lower(), source_system.lower()))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        return {str(row[0]): row[1] for row in rows}
    except Exception as e:
        print(f"Warning: Could not load event categories: {e}")
        return {}


def normalize_courtreserve_events(
    events: List[Dict],
    client_code: str,
) -> List[Dict]:
    """
    Normalize CourtReserve events to database format.
    
    Args:
        events: List of event dictionaries from CourtReserve API
        client_code: Client code (e.g., 'pklyn')
    
    Returns:
        List of normalized event dictionaries
    """
    # Load event categories once for all events
    event_categories = _load_event_categories(client_code, "courtreserve")
    
    normalized = []
    
    for event in events:
        # Extract event ID
        event_id = str(event.get("Id") or event.get("id") or event.get("EventId") or "")
        if not event_id:
            continue
        
        # Extract event name
        event_name = event.get("Name") or event.get("name") or event.get("EventName") or ""
        
        # Extract event type by looking up EventCategoryId in facility_event_categories table
        event_type = ""
        event_category_id = event.get("EventCategoryId")
        if event_category_id is not None:
            category_id_str = str(event_category_id)
            event_type = event_categories.get(category_id_str, "")
        
        # Fallback to CategoryName if lookup didn't work
        if not event_type:
            event_type = event.get("CategoryName") or event.get("EventType") or ""
        
        # Extract start and end times
        start_time_str = event.get("StartTime") or event.get("startTime") or event.get("StartDateTime")
        end_time_str = event.get("EndTime") or event.get("endTime") or event.get("EndDateTime")
        
        event_start_time = None
        event_end_time = None
        
        if start_time_str:
            try:
                if isinstance(start_time_str, str):
                    # CourtReserve uses ISO format
                    if start_time_str.endswith("Z"):
                        event_start_time = datetime.fromisoformat(
                            start_time_str.replace("Z", "+00:00")
                        )
                    else:
                        event_start_time = datetime.fromisoformat(start_time_str)
                elif isinstance(start_time_str, datetime):
                    event_start_time = start_time_str
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse start_time '{start_time_str}': {e}")
        
        if end_time_str:
            try:
                if isinstance(end_time_str, str):
                    if end_time_str.endswith("Z"):
                        event_end_time = datetime.fromisoformat(
                            end_time_str.replace("Z", "+00:00")
                        )
                    else:
                        event_end_time = datetime.fromisoformat(end_time_str)
                elif isinstance(end_time_str, datetime):
                    event_end_time = end_time_str
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse end_time '{end_time_str}': {e}")
        
        # Ensure timezone-aware datetimes
        if event_start_time and event_start_time.tzinfo is None:
            event_start_time = event_start_time.replace(tzinfo=timezone.utc)
        if event_end_time and event_end_time.tzinfo is None:
            event_end_time = event_end_time.replace(tzinfo=timezone.utc)
        
        # Extract registrant counts
        # CourtReserve: RegisteredCount and MaxRegistrants are directly in the event
        num_registrants = event.get("RegisteredCount")
        if num_registrants is not None:
            try:
                num_registrants = int(num_registrants)
            except (ValueError, TypeError):
                num_registrants = None
        
        max_registrants = event.get("MaxRegistrants")
        if max_registrants is not None:
            try:
                max_registrants = int(max_registrants)
            except (ValueError, TypeError):
                max_registrants = None
        
        # Extract admission rates from PriceInfo
        # Regular: Look for "Non-Member Account"
        # Member: Look for "Premium" or "Founder"
        admission_rate_regular = None
        admission_rate_member = None
        price_info = event.get("PriceInfo", [])
        if isinstance(price_info, list):
            for price_item in price_info:
                if not isinstance(price_item, dict):
                    continue
                membership_type = price_item.get("MembershipTypeName", "")
                # Try to get EntireEventPrice first, fallback to DailyPrice
                price = price_item.get("EntireEventPrice") or price_item.get("DailyPrice")
                
                if price is not None:
                    try:
                        price_float = float(price)
                        # Regular admission: "Non-Member Account"
                        if membership_type == "Non-Member Account":
                            admission_rate_regular = price_float
                        # Member admission: "Premium" or "Founder"
                        elif membership_type in ("Premium", "Founder"):
                            # Use the first member price we find, or the lowest if multiple
                            if admission_rate_member is None or price_float < admission_rate_member:
                                admission_rate_member = price_float
                    except (ValueError, TypeError):
                        pass
        
        normalized.append({
            "client_code": client_code.lower(),
            "source_system": "courtreserve",
            "event_id": event_id,
            "event_name": event_name,
            "event_type": event_type,
            "event_start_time": event_start_time,
            "event_end_time": event_end_time,
            "num_registrants": num_registrants,
            "max_registrants": max_registrants,
            "admission_rate_regular": admission_rate_regular,
            "admission_rate_member": admission_rate_member,
        })
    
    return normalized

