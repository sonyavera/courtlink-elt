"""add_google_photo_name_to_organizations

Revision ID: add_google_photo_name
Revises: refactor_facility_reviews
Create Date: 2025-01-15 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'add_google_photo_name'
down_revision: Union[str, None] = 'refactor_facility_reviews'
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

    # Add google_photo_name to organizations table
    # Note: This stores the photo name/reference, not a direct URL
    # The URL requires an API key header and cannot be used directly in browsers
    if _table_exists("organizations", schema):
        if not _column_exists("organizations", "google_photo_name", schema):
            op.add_column(
                "organizations",
                sa.Column("google_photo_name", sa.Text(), nullable=True),
                schema=schema,
            )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Remove google_photo_name from organizations
    if _table_exists("organizations", schema):
        if _column_exists("organizations", "google_photo_name", schema):
            op.drop_column("organizations", "google_photo_name", schema=schema)

