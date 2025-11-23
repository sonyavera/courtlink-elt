"""Normalize Podplay events for database storage."""

from datetime import datetime, timezone
from typing import Dict, List


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
        
        # Extract event type (using subtype from Podplay API)
        event_type = event.get("subtype") or ""
        
        # Extract start and end times
        start_time_str = event.get("startTime") or event.get("start_time")
        end_time_str = event.get("endTime") or event.get("end_time")
        
        event_start_time = None
        event_end_time = None
        
        if start_time_str:
            try:
                if isinstance(start_time_str, str):
                    # Handle ISO format strings
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
            "event_type": event_type,
            "event_start_time": event_start_time,
            "event_end_time": event_end_time,
            "num_registrants": num_registrants,
            "max_registrants": max_registrants,
            "admission_rate_regular": admission_rate_regular,
            "admission_rate_member": admission_rate_member,
        })
    
    return normalized

