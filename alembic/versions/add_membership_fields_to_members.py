"""add_membership_fields_to_members

Revision ID: add_membership_fields_to_members
Revises: add_has_bar_and_facility_phone
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
revision: str = "add_membership_fields_to_members"
down_revision: Union[str, None] = "add_has_bar_and_facility_phone"
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

    # Add columns to members_raw table
    if _table_exists("members_raw", schema):
        if not _column_exists("members_raw", "membership_type_name", schema):
            op.add_column(
                "members_raw",
                sa.Column("membership_type_name", sa.Text(), nullable=True),
                schema=schema,
            )
        if not _column_exists("members_raw", "is_premium_member", schema):
            op.add_column(
                "members_raw",
                sa.Column("is_premium_member", sa.Integer(), nullable=True),
                schema=schema,
            )
        if not _column_exists("members_raw", "member_since", schema):
            op.add_column(
                "members_raw",
                sa.Column("member_since", sa.Date(), nullable=True),
                schema=schema,
            )

    # Add columns to members_raw_stg table
    if _table_exists("members_raw_stg", schema):
        if not _column_exists("members_raw_stg", "membership_type_name", schema):
            op.add_column(
                "members_raw_stg",
                sa.Column("membership_type_name", sa.Text(), nullable=True),
                schema=schema,
            )
        if not _column_exists("members_raw_stg", "is_premium_member", schema):
            op.add_column(
                "members_raw_stg",
                sa.Column("is_premium_member", sa.Integer(), nullable=True),
                schema=schema,
            )
        if not _column_exists("members_raw_stg", "member_since", schema):
            op.add_column(
                "members_raw_stg",
                sa.Column("member_since", sa.Date(), nullable=True),
                schema=schema,
            )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Remove columns from members_raw_stg table
    if _table_exists("members_raw_stg", schema):
        if _column_exists("members_raw_stg", "member_since", schema):
            op.drop_column("members_raw_stg", "member_since", schema=schema)
        if _column_exists("members_raw_stg", "is_premium_member", schema):
            op.drop_column("members_raw_stg", "is_premium_member", schema=schema)
        if _column_exists("members_raw_stg", "membership_type_name", schema):
            op.drop_column("members_raw_stg", "membership_type_name", schema=schema)

    # Remove columns from members_raw table
    if _table_exists("members_raw", schema):
        if _column_exists("members_raw", "member_since", schema):
            op.drop_column("members_raw", "member_since", schema=schema)
        if _column_exists("members_raw", "is_premium_member", schema):
            op.drop_column("members_raw", "is_premium_member", schema=schema)
        if _column_exists("members_raw", "membership_type_name", schema):
            op.drop_column("members_raw", "membership_type_name", schema=schema)

