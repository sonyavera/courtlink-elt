"""
Data migration script to backfill cancellation metadata.
This corresponds to the UPDATE statements in 002_update_reservations.sql

Run this after applying the 002_update_reservations Alembic migration.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

pg_dsn = os.getenv("PG_DSN")
pg_schema = os.getenv("PG_SCHEMA")

if not pg_dsn:
    raise ValueError("PG_DSN environment variable must be set")
if not pg_schema:
    raise ValueError("PG_SCHEMA environment variable must be set")

with psycopg2.connect(pg_dsn) as conn:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{pg_schema}"')
        
        # Backfill reservation_cancellations_raw
        cur.execute(f"""
            UPDATE {pg_schema}.reservation_cancellations_raw
            SET
                client_code = COALESCE(
                    NULLIF(client_code, ''),
                    NULLIF(split_part(event_id, ':', 1), ''),
                    NULLIF(split_part(reservation_id, ':', 1), ''),
                    NULLIF(split_part(member_id, ':', 1), '')
                ),
                source_system = COALESCE(source_system, 'courtreserve'),
                event_id = CASE
                    WHEN event_id LIKE '%:%' THEN NULLIF(split_part(event_id, ':', 2), '')
                    ELSE event_id
                END,
                reservation_id = CASE
                    WHEN reservation_id LIKE '%:%' THEN NULLIF(split_part(reservation_id, ':', 2), '')
                    ELSE reservation_id
                END,
                member_id = CASE
                    WHEN member_id LIKE '%:%' THEN NULLIF(split_part(member_id, ':', 2), '')
                    ELSE member_id
                END
        """)
        print(f"Updated {cur.rowcount} rows in reservation_cancellations_raw")
        
        # Backfill reservation_cancellations_raw_stg
        cur.execute(f"""
            UPDATE {pg_schema}.reservation_cancellations_raw_stg
            SET
                client_code = COALESCE(
                    NULLIF(client_code, ''),
                    NULLIF(split_part(event_id, ':', 1), ''),
                    NULLIF(split_part(reservation_id, ':', 1), ''),
                    NULLIF(split_part(member_id, ':', 1), '')
                ),
                source_system = COALESCE(source_system, 'courtreserve'),
                event_id = CASE
                    WHEN event_id LIKE '%:%' THEN NULLIF(split_part(event_id, ':', 2), '')
                    ELSE event_id
                END,
                reservation_id = CASE
                    WHEN reservation_id LIKE '%:%' THEN NULLIF(split_part(reservation_id, ':', 2), '')
                    ELSE reservation_id
                END,
                member_id = CASE
                    WHEN member_id LIKE '%:%' THEN NULLIF(split_part(member_id, ':', 2), '')
                    ELSE member_id
                END
        """)
        print(f"Updated {cur.rowcount} rows in reservation_cancellations_raw_stg")
        
print("Data migration completed successfully")

