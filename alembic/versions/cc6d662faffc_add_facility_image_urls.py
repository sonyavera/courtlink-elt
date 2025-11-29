"""add_facility_image_urls

Revision ID: cc6d662faffc
Revises: remove_court_type_cols
Create Date: 2025-11-29 16:49:10.775862

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'cc6d662faffc'
down_revision: Union[str, None] = 'remove_court_type_cols'
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

    # Add facility_header_image_url and facility_logo_image_url to facility_details table
    if _table_exists("facility_details", schema):
        if not _column_exists("facility_details", "facility_header_image_url", schema):
            op.add_column(
                "facility_details",
                sa.Column("facility_header_image_url", sa.Text(), nullable=True),
                schema=schema,
            )
        if not _column_exists("facility_details", "facility_logo_image_url", schema):
            op.add_column(
                "facility_details",
                sa.Column("facility_logo_image_url", sa.Text(), nullable=True),
                schema=schema,
            )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Remove facility_header_image_url and facility_logo_image_url columns
    if _table_exists("facility_details", schema):
        if _column_exists("facility_details", "facility_header_image_url", schema):
            op.drop_column("facility_details", "facility_header_image_url", schema=schema)
        if _column_exists("facility_details", "facility_logo_image_url", schema):
            op.drop_column("facility_details", "facility_logo_image_url", schema=schema)

