"""update_facility_details_add_columns_remove_ownership_type

Revision ID: update_facility_details
Revises: create_facility_google_reviews
Create Date: 2025-01-15 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'update_facility_details'
down_revision: Union[str, None] = 'create_facility_google_reviews'
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

    if not _table_exists("facility_details", schema):
        return

    # Rename has_locker_rooms to has_lockers
    if _column_exists("facility_details", "has_locker_rooms", schema):
        op.alter_column(
            "facility_details",
            "has_locker_rooms",
            new_column_name="has_lockers",
            schema=schema,
        )

    # Add has_dink_court if it doesn't exist
    if not _column_exists("facility_details", "has_dink_court", schema):
        op.add_column(
            "facility_details",
            sa.Column("has_dink_court", sa.Boolean(), nullable=True),
            schema=schema,
        )

    # Add has_workout_area if it doesn't exist
    if not _column_exists("facility_details", "has_workout_area", schema):
        op.add_column(
            "facility_details",
            sa.Column("has_workout_area", sa.Boolean(), nullable=True),
            schema=schema,
        )

    # Remove ownership_type column
    if _column_exists("facility_details", "ownership_type", schema):
        op.drop_column("facility_details", "ownership_type", schema=schema)


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _table_exists("facility_details", schema):
        return

    # Rename has_lockers back to has_locker_rooms
    if _column_exists("facility_details", "has_lockers", schema):
        op.alter_column(
            "facility_details",
            "has_lockers",
            new_column_name="has_locker_rooms",
            schema=schema,
        )

    # Remove has_dink_court
    if _column_exists("facility_details", "has_dink_court", schema):
        op.drop_column("facility_details", "has_dink_court", schema=schema)

    # Remove has_workout_area
    if _column_exists("facility_details", "has_workout_area", schema):
        op.drop_column("facility_details", "has_workout_area", schema=schema)

    # Add back ownership_type
    if not _column_exists("facility_details", "ownership_type", schema):
        op.add_column(
            "facility_details",
            sa.Column("ownership_type", sa.Text(), nullable=True),
            schema=schema,
        )

