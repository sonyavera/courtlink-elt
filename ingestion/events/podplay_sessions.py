"""Normalize Podplay court availability sessions for database storage."""

from datetime import datetime, timezone
from typing import Dict, List


def normalize_podplay_sessions(
    sessions: List[Dict],
    client_code: str,
    end_time: datetime,
) -> List[Dict]:
    """
    Normalize Podplay sessions to database format.
    
    Each session represents a 30-minute time slot. For sessions with status "AVAILABLE",
    we create a row for each available court in pod.tables.items.
    
    Args:
        sessions: List of session dictionaries from Podplay API
        client_code: Client code (e.g., 'gotham')
        end_time: End time for filtering (stop processing if session startTime exceeds this)
    
    Returns:
        List of normalized session dictionaries (one per available court per time slot)
    """
    normalized = []
    
    print(f"[NORMALIZE SESSIONS] Processing {len(sessions)} sessions")
    
    status_counts = {}
    sessions_with_tables = 0
    sessions_available = 0
    sessions_beyond_end_time = 0
    
    for session in sessions:
        # Track status distribution
        session_status = session.get("status")
        status_counts[session_status] = status_counts.get(session_status, 0) + 1
        
        # Only process sessions with status "AVAILABLE" - skip "FULL"
        if session_status != "AVAILABLE":
            continue
        
        sessions_available += 1
        
        # Extract start and end times
        start_time_str = session.get("startTime")
        end_time_str = session.get("endTime")
        
        slot_start = None
        slot_end = None
        
        if start_time_str:
            try:
                if isinstance(start_time_str, str):
                    if start_time_str.endswith("Z"):
                        slot_start = datetime.fromisoformat(
                            start_time_str.replace("Z", "+00:00")
                        )
                    else:
                        slot_start = datetime.fromisoformat(start_time_str)
                elif isinstance(start_time_str, datetime):
                    slot_start = start_time_str
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse start_time '{start_time_str}': {e}")
                continue
        
        if end_time_str:
            try:
                if isinstance(end_time_str, str):
                    if end_time_str.endswith("Z"):
                        slot_end = datetime.fromisoformat(
                            end_time_str.replace("Z", "+00:00")
                        )
                    else:
                        slot_end = datetime.fromisoformat(end_time_str)
                elif isinstance(end_time_str, datetime):
                    slot_end = end_time_str
            except (ValueError, AttributeError) as e:
                print(f"Warning: Could not parse end_time '{end_time_str}': {e}")
                continue
        
        # Skip if we don't have both times
        if not slot_start or not slot_end:
            continue
        
        # Stop processing if this session is beyond our end_time
        if slot_start > end_time:
            sessions_beyond_end_time += 1
            if sessions_beyond_end_time == 1:
                print(
                    f"[NORMALIZE SESSIONS] Found session beyond end_time: "
                    f"{slot_start.isoformat()} > {end_time.isoformat()}, stopping"
                )
            break
        
        # Ensure timezone-aware datetimes
        if slot_start.tzinfo is None:
            slot_start = slot_start.replace(tzinfo=timezone.utc)
        if slot_end.tzinfo is None:
            slot_end = slot_end.replace(tzinfo=timezone.utc)
        
        # Extract period type
        period_type = session.get("periodType")  # "PEAK" or "OFF_PEAK"
        
        # Get available tables - these are the courts available for this time slot
        available_tables = session.get("availableTables", {})
        table_items = available_tables.get("items", [])
        
        if not table_items:
            continue
        
        sessions_with_tables += 1
        
        # Create a row for each available court in this time slot
        tables_processed = 0
        for available_table in table_items:
            # Only process FIXED_TABLE types (AUTO types have table=null)
            table_type = available_table.get("type")
            if table_type != "FIXED_TABLE":
                continue
            
            # Get the table/court information
            table_obj = available_table.get("table")
            
            # If table is null, skip (shouldn't happen for FIXED_TABLE, but be safe)
            if not table_obj:
                continue
            
            # Get court ID and name from the table object
            court_id = table_obj.get("id")
            court_name = table_obj.get("displayName") or table_obj.get("displayNameShort")
            
            if not court_id:
                continue
            
            normalized.append({
                "client_code": client_code.lower(),
                "source_system": "podplay",
                "court_id": str(court_id),
                "court_name": court_name,
                "slot_start": slot_start,
                "slot_end": slot_end,
                "period_type": period_type,
            })
            tables_processed += 1
        
        if tables_processed == 0 and sessions_with_tables <= 3:
            # Debug: Show why no tables were processed for first few sessions
            print(
                f"[NORMALIZE SESSIONS] Session {slot_start.isoformat()} has {len(table_items)} availableTables, "
                f"but none with FIXED_TABLE type. Available table types: "
                f"{[at.get('type') for at in table_items[:3]]}"
            )
    
    print(
        f"[NORMALIZE SESSIONS] Summary: "
        f"total={len(sessions)}, "
        f"status_counts={status_counts}, "
        f"available={sessions_available}, "
        f"with_tables={sessions_with_tables}, "
        f"beyond_end_time={sessions_beyond_end_time}, "
        f"normalized={len(normalized)}"
    )
    
    return normalized
