"""
Script to seed designer data for testing

Creates:
- Tatiana Tkachenko account at gotham and pklyn in dim_members
- Past reservations at pklyn with existing members + Tatiana
- Upcoming reservations at pklyn with existing members + Tatiana
"""

import os
import hashlib
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from ingestion.clients import PostgresClient

load_dotenv()


def generate_member_pk(client_code: str, external_member_id: str) -> str:
    """Generate member_pk hash similar to existing format"""
    combined = f"{client_code}:{external_member_id}"
    return hashlib.md5(combined.encode()).hexdigest()


def generate_reservation_pk(client_code: str, reservation_id: str) -> str:
    """Generate reservation_pk hash similar to existing format"""
    combined = f"{client_code}:{reservation_id}"
    return hashlib.md5(combined.encode()).hexdigest()


def seed_designer_data():
    """Seed data for designer testing"""
    elt_schema = (
        os.getenv("PG_SCHEMA_STG") or os.getenv("PG_SCHEMA") or "courtlink_elt_dev"
    )
    pg_dsn = os.getenv("PG_DSN")

    if not pg_dsn:
        raise RuntimeError("PG_DSN must be set in environment")

    pg_client = PostgresClient(pg_dsn, elt_schema)

    # Tatiana's info
    tatiana_email = "test@mindeddesign.com"
    tatiana_first_name = "Tatiana"
    tatiana_last_name = "Tkachenko"

    # External member IDs for Tatiana (generating unique ones)
    tatiana_gotham_id = "TatianaGotham2024"
    tatiana_pklyn_id = "TatianaPklyn2024"

    # Existing members' IDs from the database
    existing_members = {
        "svg145@gmail.com": {"pklyn": "5560160"},
        "andrew.laprise@gmail.com": {"pklyn": "6259061"},
        "will@wflewis.com": {"pklyn": "8104005"},
        "daeko183@gmail.com": {"pklyn": "8104006"},
    }

    with pg_client._connect() as conn, conn.cursor() as cur:
        print(f"🌱 Seeding designer data in {elt_schema}...")

        # 1. Create Tatiana in dim_members for gotham
        tatiana_gotham_pk = generate_member_pk("gotham", tatiana_gotham_id)
        print(f"   Creating Tatiana at gotham (member_pk: {tatiana_gotham_pk})...")
        # Check if already exists
        cur.execute(
            f"""
            SELECT member_pk FROM {elt_schema}.dim_members 
            WHERE member_pk = %s
        """,
            (tatiana_gotham_pk,),
        )
        existing = cur.fetchone()

        if not existing:
            cur.execute(
                f"""
                INSERT INTO {elt_schema}.dim_members 
                (member_pk, client_code, external_member_id, first_name, last_name, email, created_at, reservation_system_code)
                VALUES 
                (%s, 'gotham', %s, %s, %s, %s, NOW(), 'podplay')
            """,
                (
                    tatiana_gotham_pk,
                    tatiana_gotham_id,
                    tatiana_first_name,
                    tatiana_last_name,
                    tatiana_email,
                ),
            )

        # 2. Create Tatiana in dim_members for pklyn
        tatiana_pklyn_pk = generate_member_pk("pklyn", tatiana_pklyn_id)
        print(f"   Creating Tatiana at pklyn (member_pk: {tatiana_pklyn_pk})...")
        # Check if already exists
        cur.execute(
            f"""
            SELECT member_pk FROM {elt_schema}.dim_members 
            WHERE member_pk = %s
        """,
            (tatiana_pklyn_pk,),
        )
        existing2 = cur.fetchone()

        if not existing2:
            cur.execute(
                f"""
                INSERT INTO {elt_schema}.dim_members 
                (member_pk, client_code, external_member_id, first_name, last_name, email, created_at, reservation_system_code)
                VALUES 
                (%s, 'pklyn', %s, %s, %s, %s, NOW(), 'courtreserve')
            """,
                (
                    tatiana_pklyn_pk,
                    tatiana_pklyn_id,
                    tatiana_first_name,
                    tatiana_last_name,
                    tatiana_email,
                ),
            )

        # 3. Create past reservations (3-4 weeks ago)
        print(f"   Creating past reservations...")
        now = datetime.now(timezone.utc)

        # Past reservation 1 (3 weeks ago, 7pm-8pm)
        past_date_1 = now - timedelta(weeks=3)
        past_start_1 = past_date_1.replace(hour=19, minute=0, second=0, microsecond=0)
        past_end_1 = past_start_1 + timedelta(hours=1)

        reservation_id_1 = "27504476"
        reservation_pk_1 = generate_reservation_pk("pklyn", reservation_id_1)

        # Create reservation with all members
        # reservation_pk is the same for all members on the same reservation
        reservation_pk_1 = generate_reservation_pk("pklyn", reservation_id_1)

        members_for_res_1 = [
            ("svg145@gmail.com", existing_members["svg145@gmail.com"]["pklyn"]),
            (
                "andrew.laprise@gmail.com",
                existing_members["andrew.laprise@gmail.com"]["pklyn"],
            ),
            ("will@wflewis.com", existing_members["will@wflewis.com"]["pklyn"]),
            ("daeko183@gmail.com", existing_members["daeko183@gmail.com"]["pklyn"]),
            (tatiana_email, tatiana_pklyn_id),
        ]

        for email, member_id in members_for_res_1:
            # Check if already exists
            cur.execute(
                f"""
                SELECT reservation_pk FROM {elt_schema}.fct_reservations 
                WHERE reservation_id = %s AND member_id = %s
            """,
                (reservation_id_1, member_id),
            )
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    f"""
                    INSERT INTO {elt_schema}.fct_reservations 
                    (reservation_pk, client_code, reservation_system_code, source_system_code, 
                     reservation_id, event_id, member_id, reservation_created_at, reservation_updated_at,
                     reservation_start_at, reservation_end_at, ingested_at)
                    VALUES 
                    (%s, 'pklyn', 'courtreserve', 'courtreserve',
                     %s, %s, %s, %s, %s,
                     %s, %s, NOW())
                """,
                    (
                        reservation_pk_1,
                        reservation_id_1,
                        "57793",
                        member_id,
                        past_start_1 - timedelta(days=1),
                        past_start_1 - timedelta(hours=1),
                        past_start_1,
                        past_end_1,
                    ),
                )

        # Past reservation 2 (2 weeks ago, 6pm-7pm)
        past_date_2 = now - timedelta(weeks=2)
        past_start_2 = past_date_2.replace(hour=18, minute=0, second=0, microsecond=0)
        past_end_2 = past_start_2 + timedelta(hours=1)

        reservation_id_2 = "27504477"
        reservation_pk_2 = generate_reservation_pk("pklyn", reservation_id_2)

        members_for_res_2 = [
            ("svg145@gmail.com", existing_members["svg145@gmail.com"]["pklyn"]),
            (
                "andrew.laprise@gmail.com",
                existing_members["andrew.laprise@gmail.com"]["pklyn"],
            ),
            (tatiana_email, tatiana_pklyn_id),
        ]

        for email, member_id in members_for_res_2:
            # Check if already exists
            cur.execute(
                f"""
                SELECT reservation_pk FROM {elt_schema}.fct_reservations 
                WHERE reservation_id = %s AND member_id = %s
            """,
                (reservation_id_2, member_id),
            )
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    f"""
                    INSERT INTO {elt_schema}.fct_reservations 
                    (reservation_pk, client_code, reservation_system_code, source_system_code, 
                     reservation_id, event_id, member_id, reservation_created_at, reservation_updated_at,
                     reservation_start_at, reservation_end_at, ingested_at)
                    VALUES 
                    (%s, 'pklyn', 'courtreserve', 'courtreserve',
                     %s, %s, %s, %s, %s,
                     %s, %s, NOW())
                """,
                    (
                        reservation_pk_2,
                        reservation_id_2,
                        "57794",
                        member_id,
                        past_start_2 - timedelta(days=1),
                        past_start_2 - timedelta(hours=1),
                        past_start_2,
                        past_end_2,
                    ),
                )

        # 4. Create upcoming reservations (next week and week after)
        print(f"   Creating upcoming reservations...")

        # Upcoming reservation 1 (next week, Tuesday 7pm-8pm)
        next_week = now + timedelta(weeks=1)
        # Find next Tuesday
        days_until_tuesday = (1 - next_week.weekday()) % 7
        if days_until_tuesday == 0:
            days_until_tuesday = 7
        upcoming_date_1 = next_week + timedelta(days=days_until_tuesday)
        upcoming_start_1 = upcoming_date_1.replace(
            hour=19, minute=0, second=0, microsecond=0
        )
        upcoming_end_1 = upcoming_start_1 + timedelta(hours=1)

        reservation_id_3 = "27504478"
        reservation_pk_3 = generate_reservation_pk("pklyn", reservation_id_3)

        members_for_res_3 = [
            ("svg145@gmail.com", existing_members["svg145@gmail.com"]["pklyn"]),
            (
                "andrew.laprise@gmail.com",
                existing_members["andrew.laprise@gmail.com"]["pklyn"],
            ),
            ("will@wflewis.com", existing_members["will@wflewis.com"]["pklyn"]),
            ("daeko183@gmail.com", existing_members["daeko183@gmail.com"]["pklyn"]),
            (tatiana_email, tatiana_pklyn_id),
        ]

        for email, member_id in members_for_res_3:
            # Check if already exists
            cur.execute(
                f"""
                SELECT reservation_pk FROM {elt_schema}.fct_reservations 
                WHERE reservation_id = %s AND member_id = %s
            """,
                (reservation_id_3, member_id),
            )
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    f"""
                    INSERT INTO {elt_schema}.fct_reservations 
                    (reservation_pk, client_code, reservation_system_code, source_system_code, 
                     reservation_id, event_id, member_id, reservation_created_at, reservation_updated_at,
                     reservation_start_at, reservation_end_at, ingested_at)
                    VALUES 
                    (%s, 'pklyn', 'courtreserve', 'courtreserve',
                     %s, %s, %s, %s, %s,
                     %s, %s, NOW())
                """,
                    (
                        reservation_pk_3,
                        reservation_id_3,
                        "57795",
                        member_id,
                        now - timedelta(days=2),
                        now - timedelta(hours=1),
                        upcoming_start_1,
                        upcoming_end_1,
                    ),
                )

        # Upcoming reservation 2 (2 weeks from now, Thursday 6pm-7pm)
        two_weeks = now + timedelta(weeks=2)
        days_until_thursday = (3 - two_weeks.weekday()) % 7
        if days_until_thursday == 0:
            days_until_thursday = 7
        upcoming_date_2 = two_weeks + timedelta(days=days_until_thursday)
        upcoming_start_2 = upcoming_date_2.replace(
            hour=18, minute=0, second=0, microsecond=0
        )
        upcoming_end_2 = upcoming_start_2 + timedelta(hours=1)

        reservation_id_4 = "27504479"
        reservation_pk_4 = generate_reservation_pk("pklyn", reservation_id_4)

        members_for_res_4 = [
            ("svg145@gmail.com", existing_members["svg145@gmail.com"]["pklyn"]),
            ("will@wflewis.com", existing_members["will@wflewis.com"]["pklyn"]),
            (tatiana_email, tatiana_pklyn_id),
        ]

        for email, member_id in members_for_res_4:
            # Check if already exists
            cur.execute(
                f"""
                SELECT reservation_pk FROM {elt_schema}.fct_reservations 
                WHERE reservation_id = %s AND member_id = %s
            """,
                (reservation_id_4, member_id),
            )
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    f"""
                    INSERT INTO {elt_schema}.fct_reservations 
                    (reservation_pk, client_code, reservation_system_code, source_system_code, 
                     reservation_id, event_id, member_id, reservation_created_at, reservation_updated_at,
                     reservation_start_at, reservation_end_at, ingested_at)
                    VALUES 
                    (%s, 'pklyn', 'courtreserve', 'courtreserve',
                     %s, %s, %s, %s, %s,
                     %s, %s, NOW())
                """,
                    (
                        reservation_pk_4,
                        reservation_id_4,
                        "57796",
                        member_id,
                        now - timedelta(days=1),
                        now - timedelta(hours=1),
                        upcoming_start_2,
                        upcoming_end_2,
                    ),
                )

        conn.commit()

        print(f"✅ Designer data seeded successfully!")
        print(f"   - Created Tatiana Tkachenko at gotham and pklyn")
        print(f"   - Created 2 past reservations")
        print(f"   - Created 2 upcoming reservations")


if __name__ == "__main__":
    seed_designer_data()
