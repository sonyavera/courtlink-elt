"""add_description_to_facility_events_raw_stg

Revision ID: add_description_stg
Revises: add_description_facility_events
Create Date: 2025-11-26 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'add_description_stg'
down_revision: Union[str, None] = 'add_description_facility_events'
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
        # Fallback to direct SQL query
        try:
            if schema:
                query = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_schema = :schema
                        AND table_name = :table_name
                        AND column_name = :column_name
                    )
                """)
                result = bind.execute(query, {
                    "schema": schema,
                    "table_name": table_name,
                    "column_name": column_name
                })
            else:
                query = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_name = :table_name
                        AND column_name = :column_name
                    )
                """)
                result = bind.execute(query, {
                    "table_name": table_name,
                    "column_name": column_name
                })
            return result.scalar()
        except Exception:
            return False


def upgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Add column to staging table if it exists and doesn't have the column
    if _table_exists('facility_events_raw_stg', schema):
        if not _column_exists('facility_events_raw_stg', 'event_description', schema):
            op.add_column('facility_events_raw_stg', sa.Column('event_description', sa.Text(), nullable=True))
            print("[MIGRATION] Added event_description column to facility_events_raw_stg")
        else:
            print("[MIGRATION] facility_events_raw_stg already has event_description column")
    else:
        print("[MIGRATION] facility_events_raw_stg table does not exist, skipping")


def downgrade() -> None:
    # Drop column from staging table if it exists
    try:
        op.drop_column('facility_events_raw_stg', 'event_description')
    except Exception:
        pass  # Table might not exist

