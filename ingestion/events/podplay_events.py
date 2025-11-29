"""Normalize Podplay events for database storage."""

from typing import Dict, List

from ingestion.utils.datetime import to_utc_datetime
from ingestion.utils.timezones import resolve_timezone

DEFAULT_PODPLAY_TIMEZONE = "UTC"


def normalize_podplay_events(
    events: List[Dict],
    client_code: str,
) -> List[Dict]:
    """
    Normalize Podplay events to database format.
    
    Args:
        events: List of event dictionaries from Podplay API
        client_code: Client code (e.g., 'gotham')
    
    Returns:
        List of normalized event dictionaries
    """
    normalized = []
    
    for event in events:
        # Extract event ID
        event_id = str(event.get("id", ""))
        if not event_id:
            continue
        
        # Extract event name
        event_name = event.get("name") or event.get("title") or ""
        
        # Extract event description - ensure we get the full string including newlines
        event_description = event.get("description")
        if event_description is None:
            event_description = ""
        else:
            # Ensure it's a string and preserve all content including newlines
            event_description = str(event_description)
        
        # Extract event type (using subtype from Podplay API)
        event_type = event.get("subtype") or ""
        
        _, tzinfo = resolve_timezone(
            explicit=event.get("timezone"),
            client_code=client_code,
            default=DEFAULT_PODPLAY_TIMEZONE,
        )
        start_time_raw = event.get("startTime") or event.get("start_time")
        end_time_raw = event.get("endTime") or event.get("end_time")

        event_start_time = to_utc_datetime(start_time_raw, tzinfo)
        event_end_time = to_utc_datetime(end_time_raw, tzinfo)
        
        # Extract registrant counts
        # Podplay: signups._total gives number of registrants
        signups = event.get("signups", {})
        num_registrants = None
        if isinstance(signups, dict):
            num_registrants = signups.get("_total")
            if num_registrants is not None:
                try:
                    num_registrants = int(num_registrants)
                except (ValueError, TypeError):
                    num_registrants = None
        
        # Podplay: max registrants = totalTeams * teamSize
        max_registrants = None
        total_teams = event.get("totalTeams")
        team_size = event.get("teamSize")
        if total_teams is not None and team_size is not None:
            try:
                max_registrants = int(total_teams) * int(team_size)
            except (ValueError, TypeError):
                max_registrants = None
        
        # Extract admission rates
        admission_rate_regular = None
        admission_rate_member = None
        admission_rate = event.get("admissionRate", {})
        if isinstance(admission_rate, dict):
            regular = admission_rate.get("regular")
            if regular is not None:
                try:
                    admission_rate_regular = float(regular)
                except (ValueError, TypeError):
                    admission_rate_regular = None
            
            member = admission_rate.get("member")
            if member is not None:
                try:
                    admission_rate_member = float(member)
                except (ValueError, TypeError):
                    admission_rate_member = None
        
        normalized.append({
            "client_code": client_code.lower(),
            "source_system": "podplay",
            "event_id": event_id,
            "event_name": event_name,
            "event_description": event_description,
            "event_type": event_type,
            "event_start_time": event_start_time,
            "event_end_time": event_end_time,
            "num_registrants": num_registrants,
            "max_registrants": max_registrants,
            "admission_rate_regular": admission_rate_regular,
            "admission_rate_member": admission_rate_member,
        })
    
    return normalized

