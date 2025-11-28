"""CourtReserve court availability calculation.

All datetime operations work in UTC for consistency with existing ingestion patterns.
CourtReserve API returns naive times which are parsed as EST and converted to UTC.
Operating hours are stored in UTC in the database (pre-converted from EST).
"""

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Set
from ingestion.utils.datetime import parse_iso_datetime
from ingestion.courtreserve.date_helpers import parse_event_time, parse_utc_time


def parse_courts_field(courts_field) -> List[str]:
    """
    Parse the Courts field from CourtReserve API.
    
    From eventlist API: Courts is an array of objects with Label field
    Example: [{"Id": 44076, "Label": "Court #1", ...}]
    
    From reservations API: Courts is a string
    Example: "Court #2" or "Court #1, Court #2"
    
    Returns: ["Court #1", "Court #2"]
    """
    if not courts_field:
        return []
    
    # Handle array format (from eventlist API)
    if isinstance(courts_field, list):
        return [court.get('Label', '').strip() for court in courts_field if court.get('Label')]
    
    # Handle string format (from reservations API)
    if isinstance(courts_field, str):
        courts = [c.strip() for c in courts_field.split(',') if c.strip()]
        return courts
    
    return []


def generate_time_slots(
    start_date: datetime,
    end_date: datetime,
    operating_hours: dict,
    slot_minutes: int = 30
) -> List[Dict]:
    """
    Generate all possible time slots based on operating hours (stored in local time).
    
    Args:
        start_date: Start date for slot generation (UTC)
        end_date: End date for slot generation (UTC)
        operating_hours: Operating hours from organizations table (JSONB, in local timezone)
        slot_minutes: Duration of each slot in minutes (default 30)
    
    Returns:
        List of dicts with slot_start and slot_end (both UTC)
    
    Note: Operating hours are stored in local facility timezone (e.g., America/New_York).
    Times are converted to UTC for storage. This automatically handles DST transitions.
    """
    if not operating_hours:
        return []
    
    # Get timezone (default to America/New_York)
    timezone_str = operating_hours.get('timezone', 'America/New_York')
    from zoneinfo import ZoneInfo
    facility_tz = ZoneInfo(timezone_str)
    
    slots = []
    current_date = start_date.date()
    end_date_date = end_date.date()
    
    # Day of week mapping
    day_map = {
        0: 'monday',
        1: 'tuesday',
        2: 'wednesday',
        3: 'thursday',
        4: 'friday',
        5: 'saturday',
        6: 'sunday'
    }
    
    while current_date <= end_date_date:
        day_of_week = day_map[current_date.weekday()]
        day_hours = operating_hours.get(day_of_week)
        
        if not day_hours:
            current_date += timedelta(days=1)
            continue
        
        open_time = day_hours.get('open')
        close_time = day_hours.get('close')
        
        if not open_time or not close_time:
            current_date += timedelta(days=1)
            continue
        
        # Parse times (format: "07:00" - in local timezone)
        open_hour, open_min = map(int, open_time.split(':'))
        close_hour, close_min = map(int, close_time.split(':'))
        
        # Create datetime objects in facility local timezone
        slot_start_local = datetime(
            current_date.year,
            current_date.month,
            current_date.day,
            open_hour,
            open_min,
            tzinfo=facility_tz
        )
        
        # Handle closing after midnight (e.g., close at 01:00 means 1am next day)
        if close_hour < open_hour:
            end_datetime_local = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                close_hour,
                close_min,
                tzinfo=facility_tz
            ) + timedelta(days=1)
        else:
            end_datetime_local = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                close_hour,
                close_min,
                tzinfo=facility_tz
            )
        
        # Convert to UTC for storage
        slot_start = slot_start_local.astimezone(timezone.utc)
        end_datetime = end_datetime_local.astimezone(timezone.utc)
        
        # Generate slots
        while slot_start < end_datetime:
            slot_end = slot_start + timedelta(minutes=slot_minutes)
            if slot_end <= end_datetime:
                slots.append({
                    'slot_start': slot_start,
                    'slot_end': slot_end
                })
            slot_start = slot_end
        
        current_date += timedelta(days=1)
    
    return slots


def calculate_available_slots(
    client_code: str,
    courts: List[Dict],
    operating_hours: dict,
    events: List[Dict],
    reservations: List[Dict],
    start_date: datetime,
    end_date: datetime
) -> List[Dict]:
    """
    Calculate available court slots by subtracting blocked times from all possible slots.
    
    All times are in UTC for consistency with existing event/reservation handling.
    Operating hours are expected to be pre-converted to UTC in the database.
    
    Returns:
        List of dicts with court_id, court_name, slot_start, slot_end (all UTC)
    """
    # Generate all possible time slots
    all_slots = generate_time_slots(start_date, end_date, operating_hours)
    
    if not all_slots:
        print(f"[COURT AVAILABILITY] No slots generated for {client_code} - check operating hours")
        return []
    
    print(f"[COURT AVAILABILITY] Generated {len(all_slots)} possible time slots")
    
    # Create a set of blocked (court, slot) tuples
    blocked_slots: Set[tuple] = set()
    
    # Process events that block courts
    for event in events:
        courts_str = event.get('Courts', '')
        if not courts_str:
            continue
        
        event_courts = parse_courts_field(courts_str)
        start_time = parse_event_time(event.get('StartDateTime'))
        end_time = parse_event_time(event.get('EndDateTime'))
        
        if not start_time or not end_time:
            continue
        
        # Mark all slots during this event as blocked for these courts
        for court_label in event_courts:
            for slot in all_slots:
                # Check if slot overlaps with event
                if slot['slot_start'] < end_time and slot['slot_end'] > start_time:
                    blocked_slots.add((court_label, slot['slot_start'], slot['slot_end']))
    
    # Process reservations that block courts
    for reservation in reservations:
        courts_str = reservation.get('Courts', '')
        if not courts_str:
            continue
        
        res_courts = parse_courts_field(courts_str)
        start_time = parse_event_time(reservation.get('StartTime'))
        end_time = parse_event_time(reservation.get('EndTime'))
        cancelled = reservation.get('CancelledOn')
        
        if cancelled or not start_time or not end_time:
            continue
        
        # Mark all slots during this reservation as blocked for these courts
        for court_label in res_courts:
            for slot in all_slots:
                # Check if slot overlaps with reservation
                if slot['slot_start'] < end_time and slot['slot_end'] > start_time:
                    blocked_slots.add((court_label, slot['slot_start'], slot['slot_end']))
    
    print(f"[COURT AVAILABILITY] Found {len(blocked_slots)} blocked slots from events/reservations")
    
    # Generate available slots (all slots minus blocked slots)
    available_slots = []
    
    for court in courts:
        court_id = str(court['id'])
        court_label = court['label']
        
        for slot in all_slots:
            # Check if this (court, slot) is blocked
            if (court_label, slot['slot_start'], slot['slot_end']) not in blocked_slots:
                available_slots.append({
                    'client_code': client_code,
                    'source_system': 'courtreserve',
                    'court_id': court_id,
                    'court_name': court_label,
                    'slot_start': slot['slot_start'],
                    'slot_end': slot['slot_end'],
                    'period_type': None  # Could be 'peak' or 'off_peak' if needed
                })
    
    print(f"[COURT AVAILABILITY] Calculated {len(available_slots)} available slots across {len(courts)} courts")
    
    return available_slots

