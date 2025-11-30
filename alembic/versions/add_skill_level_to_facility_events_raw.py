"""add_skill_level_to_facility_events_raw

Revision ID: add_skill_level_events
Revises: add_operating_hours_display
Create Date: 2025-01-27 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'add_skill_level_events'
down_revision: Union[str, None] = 'add_operating_hours_display'
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
    """Check if a column exists in a table."""
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

    # Add skill_level to facility_events_raw table
    if _table_exists("facility_events_raw", schema):
        if not _column_exists("facility_events_raw", "skill_level", schema):
            op.add_column(
                "facility_events_raw",
                sa.Column("skill_level", sa.Text(), nullable=True),
                schema=schema,
            )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Remove skill_level column
    if _table_exists("facility_events_raw", schema):
        if _column_exists("facility_events_raw", "skill_level", schema):
            op.drop_column("facility_events_raw", "skill_level", schema=schema)

