"""rename_metadata_to_facility_metadata

Revision ID: rename_metadata_column
Revises: move_google_place_id_refactor_reviews
Create Date: 2025-01-15 13:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'rename_metadata_column'
down_revision: Union[str, None] = 'move_google_place_id'
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

    # Rename metadata column to facility_metadata in facility_details table
    if _table_exists("facility_details", schema):
        if _column_exists("facility_details", "metadata", schema):
            op.alter_column(
                "facility_details",
                "metadata",
                new_column_name="facility_metadata",
                schema=schema,
            )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Rename facility_metadata back to metadata
    if _table_exists("facility_details", schema):
        if _column_exists("facility_details", "facility_metadata", schema):
            op.alter_column(
                "facility_details",
                "facility_metadata",
                new_column_name="metadata",
                schema=schema,
            )

