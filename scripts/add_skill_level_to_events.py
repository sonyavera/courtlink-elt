"""
Add skill_level column to facility_events_raw table based on event_name and event_description.

This script uses Python regex (more powerful than PostgreSQL) to detect skill levels
and updates the facility_events_raw table with the skill_level values.
"""

import os
import re
import sys
from dotenv import load_dotenv
from ingestion.clients import PostgresClient

load_dotenv()

# Skill level patterns (using Python regex)
SKILL_RULES = {
    "advanced_beginner": [
        r"\b201\b",
        r"Advanced\s*Beginner",
        r"\b3\.0\+?\b",
        r"Experienced\s*Beginner",
        r"\bAdv(?:anced)?\s*Beg(?:inner)?\b",
        r"Experienced\s+Beg",  # "Experienced Beg" (e.g., "Experienced Beg / Intermediate")
        r"\bExp\s*Beg\b",  # Standalone "Exp Beg" shorthand
    ],
    "beginner": [
        r"\b101\b",
        r"\bIntro",
        r"\bBeginner\b",
        r"\b2\.0\b",
        r"\b2\.25\b",
        r"\b2\.5\b",
        r"\b2\.75\b",
        r"Try[ -]?[Ii]t",  # Try it / Try-it / Try It / Try-It
        r"Try-?It",  # $5 Pickleball Try-It
    ],
    "high_intermediate": [
        r"High\s*Intermediate",
        r"Advanced\s*Intermediate",
        r"\b3\.75\+?\b",
    ],
    "intermediate": [
        r"\b301\b",
        r"\b302\b",
        r"\bIntermediate\b",
        r"\b3\.25\+?\b",
        r"\b3\.5\+?\b",
        r"Paddle\s*Battle",
        r"\bDrill\s*Hour\b",
        r"Fixed\s*Partner",
    ],
    "advanced": [
        r"\b401\b",
        r"\b501\b",
        r"\bAdvanced\b",
        r"\b4\.0\+?\b",
        r"\b4\.25\+?\b",
        r"\b4\.5\+?\b",
        r"\b5\.0\b",
        r"\bExpert\b",
        r"Live\s*Match\s*Play",
    ],
}


def detect_skill_level(event_name: str, event_description: str) -> str:
    """
    Detect skill level from event_name ONLY.
    Returns skill level string or "All Levels" if no match.
    """
    event_name_text = (event_name or "").strip()

    if not event_name_text:
        return "All Levels"

    # Check for explicit "All Levels" first (case-insensitive, word boundaries)
    if re.search(r"\bAll\s+Levels\b", event_name_text, re.IGNORECASE):
        return "All Levels"

    # Check patterns in priority order (most specific first)

    # 1. High Intermediate (must have "High" or "Advanced" before "Intermediate")
    if re.search(r"\b(High|Advanced)\s+Intermediate\b", event_name_text, re.IGNORECASE):
        return "High Intermediate"

    # Check for 3.75+ rating (high intermediate)
    if re.search(r"\b3\.75\+?\b", event_name_text, re.IGNORECASE):
        return "High Intermediate"

    # 2. Advanced Beginner - check for explicit phrases first
    if re.search(
        r"\b(Advanced|Experienced)\s+Beginner\b", event_name_text, re.IGNORECASE
    ):
        return "Advanced Beginner"

    # Check advanced beginner patterns (201, 3.0+, Exp Beg, etc.)
    for pattern in SKILL_RULES["advanced_beginner"]:
        if re.search(pattern, event_name_text, re.IGNORECASE):
            # Make sure it's not part of "Advanced Intermediate"
            if not re.search(
                r"Advanced\s+Intermediate", event_name_text, re.IGNORECASE
            ):
                return "Advanced Beginner"

    # 3. Advanced (standalone "Advanced" not followed by Intermediate/Beginner)
    if not re.search(
        r"Advanced\s+(Intermediate|Beginner)", event_name_text, re.IGNORECASE
    ):
        for pattern in SKILL_RULES["advanced"]:
            if re.search(pattern, event_name_text, re.IGNORECASE):
                return "Advanced"

    # 4. Intermediate (standalone "Intermediate" or specific codes/ratings)
    # Must NOT be "High Intermediate" or "Advanced Intermediate"
    if not re.search(
        r"\b(High|Advanced)\s+Intermediate\b", event_name_text, re.IGNORECASE
    ):
        for pattern in SKILL_RULES["intermediate"]:
            if re.search(pattern, event_name_text, re.IGNORECASE):
                return "Intermediate"

    # 5. Beginner (standalone "Beginner" or specific codes)
    # Must NOT be "Advanced Beginner" or "Experienced Beginner"
    if not re.search(
        r"\b(Advanced|Experienced)\s+Beginner\b", event_name_text, re.IGNORECASE
    ):
        for pattern in SKILL_RULES["beginner"]:
            if re.search(pattern, event_name_text, re.IGNORECASE):
                return "Beginner"

    return "All Levels"


def main():
    """Update skill_level column in facility_events_raw table."""
    pg_schema = os.getenv("PG_SCHEMA")
    pg_dsn = os.getenv("PG_DSN")

    if not pg_schema or not pg_dsn:
        print("Error: PG_SCHEMA and PG_DSN must be set in environment")
        sys.exit(1)

    pg_client = PostgresClient(pg_dsn, pg_schema)

    print(f"Fetching events from {pg_schema}.facility_events_raw...")

    with pg_client._connect() as conn, conn.cursor() as cur:
        # Fetch all events
        cur.execute(
            f"""
            SELECT client_code, event_id, source_system, event_start_time, 
                   event_name, event_description
            FROM {pg_schema}.facility_events_raw
        """
        )

        events = cur.fetchall()
        print(f"Found {len(events)} events to process")

        # Check if skill_level column exists, if not add it
        cur.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = 'facility_events_raw' 
            AND column_name = 'skill_level'
        """,
            (pg_schema,),
        )

        if not cur.fetchone():
            print("Adding skill_level column to facility_events_raw...")
            cur.execute(
                f"""
                ALTER TABLE {pg_schema}.facility_events_raw 
                ADD COLUMN skill_level TEXT
            """
            )
            conn.commit()

        # Update skill levels
        updated = 0
        for event in events:
            (
                client_code,
                event_id,
                source_system,
                event_start_time,
                event_name,
                event_description,
            ) = event

            skill_level = detect_skill_level(event_name, event_description)

            cur.execute(
                f"""
                UPDATE {pg_schema}.facility_events_raw
                SET skill_level = %s
                WHERE client_code = %s 
                AND event_id = %s 
                AND source_system = %s 
                AND event_start_time = %s
            """,
                (skill_level, client_code, event_id, source_system, event_start_time),
            )

            updated += 1
            if updated % 100 == 0:
                print(f"Updated {updated} events...")
                conn.commit()

        conn.commit()
        print(f"✓ Updated skill_level for {updated} events")


if __name__ == "__main__":
    main()
