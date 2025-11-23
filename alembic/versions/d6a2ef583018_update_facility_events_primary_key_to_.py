"""update_facility_events_primary_key_to_include_start_time

Revision ID: d6a2ef583018
Revises: 5daaa8576dce
Create Date: 2025-11-23 16:55:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'd6a2ef583018'
down_revision: Union[str, None] = '5daaa8576dce'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str, schema: str = None) -> bool:
    """Check if a table exists in the database."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        if schema:
            return table_name in inspector.get_table_names(schema=schema)
        return table_name in inspector.get_table_names()
    except Exception:
        return False


def _column_exists(table_name: str, column_name: str, schema: str = None) -> bool:
    """Check if a column exists in the specified table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        if schema:
            columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
        else:
            columns = [col['name'] for col in inspector.get_columns(table_name)]
        return column_name in columns
    except Exception:
        return False


def upgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")
    bind = op.get_bind()

    if not _table_exists("facility_events_raw", schema):
        # Table doesn't exist, nothing to do
        return

    # First, ensure event_start_time is NOT NULL
    # We'll need to handle any NULL values first
    if schema:
        # Check for NULL values and handle them
        bind.execute(text(f"""
            UPDATE "{schema}"."facility_events_raw"
            SET event_start_time = created_at
            WHERE event_start_time IS NULL
        """))
        
        # Now make the column NOT NULL
        bind.execute(text(f"""
            ALTER TABLE "{schema}"."facility_events_raw"
            ALTER COLUMN event_start_time SET NOT NULL
        """))
    else:
        bind.execute(text("""
            UPDATE facility_events_raw
            SET event_start_time = created_at
            WHERE event_start_time IS NULL
        """))
        
        bind.execute(text("""
            ALTER TABLE facility_events_raw
            ALTER COLUMN event_start_time SET NOT NULL
        """))

    # Drop the old primary key constraint
    if schema:
        bind.execute(text(f"""
            ALTER TABLE "{schema}"."facility_events_raw"
            DROP CONSTRAINT IF EXISTS facility_events_raw_pkey
        """))
    else:
        bind.execute(text("""
            ALTER TABLE facility_events_raw
            DROP CONSTRAINT IF EXISTS facility_events_raw_pkey
        """))

    # Create the new primary key constraint with event_start_time
    if schema:
        op.create_primary_key(
            'facility_events_raw_pkey',
            'facility_events_raw',
            ['client_code', 'source_system', 'event_id', 'event_start_time'],
            schema=schema
        )
    else:
        op.create_primary_key(
            'facility_events_raw_pkey',
            'facility_events_raw',
            ['client_code', 'source_system', 'event_id', 'event_start_time']
        )


def downgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")
    bind = op.get_bind()

    if not _table_exists("facility_events_raw", schema):
        return

    # Drop the new primary key constraint
    if schema:
        bind.execute(text(f"""
            ALTER TABLE "{schema}"."facility_events_raw"
            DROP CONSTRAINT IF EXISTS facility_events_raw_pkey
        """))
    else:
        bind.execute(text("""
            ALTER TABLE facility_events_raw
            DROP CONSTRAINT IF EXISTS facility_events_raw_pkey
        """))

    # Recreate the old primary key constraint (without event_start_time)
    if schema:
        op.create_primary_key(
            'facility_events_raw_pkey',
            'facility_events_raw',
            ['client_code', 'source_system', 'event_id'],
            schema=schema
        )
    else:
        op.create_primary_key(
            'facility_events_raw_pkey',
            'facility_events_raw',
            ['client_code', 'source_system', 'event_id']
        )

    # Make event_start_time nullable again
    if schema:
        bind.execute(text(f"""
            ALTER TABLE "{schema}"."facility_events_raw"
            ALTER COLUMN event_start_time DROP NOT NULL
        """))
    else:
        bind.execute(text("""
            ALTER TABLE facility_events_raw
            ALTER COLUMN event_start_time DROP NOT NULL
        """))
