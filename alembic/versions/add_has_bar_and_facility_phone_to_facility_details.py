"""add_has_bar_and_facility_phone_to_facility_details

Revision ID: add_has_bar_and_facility_phone
Revises: update_facility_details
Create Date: 2025-12-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = "add_has_bar_and_facility_phone"
down_revision: Union[str, None] = "add_skill_level_events"
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
            columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
        else:
            columns = [col["name"] for col in inspector.get_columns(table_name)]
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

    # Add has_bar column
    if not _column_exists("facility_details", "has_bar", schema):
        op.add_column(
            "facility_details",
            sa.Column("has_bar", sa.Boolean(), nullable=True),
            schema=schema,
        )

    # Add facility_phone_number column
    if not _column_exists("facility_details", "facility_phone_number", schema):
        op.add_column(
            "facility_details",
            sa.Column("facility_phone_number", sa.Text(), nullable=True),
            schema=schema,
        )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _table_exists("facility_details", schema):
        return

    # Drop facility_phone_number
    if _column_exists("facility_details", "facility_phone_number", schema):
        op.drop_column("facility_details", "facility_phone_number", schema=schema)

    # Drop has_bar
    if _column_exists("facility_details", "has_bar", schema):
        op.drop_column("facility_details", "has_bar", schema=schema)


