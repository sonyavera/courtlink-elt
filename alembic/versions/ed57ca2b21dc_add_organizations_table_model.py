"""add_organizations_table_model

Revision ID: ed57ca2b21dc
Revises: 9e56d762bfaa
Create Date: 2025-11-23 18:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import JSONB
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = "ed57ca2b21dc"
down_revision: Union[str, None] = "9e56d762bfaa"
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


def upgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _table_exists("organizations", schema):
        op.create_table(
            "organizations",
            sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
            sa.Column("source_system_code", sa.Text(), nullable=False),
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("login_link", sa.Text(), nullable=True),
            sa.Column("city", sa.Text(), nullable=True),
            sa.Column("is_customer", sa.Boolean(), nullable=True),
            sa.Column("hourly_rate_non_member", sa.Integer(), nullable=True),
            sa.Column("hourly_rate_member", sa.Integer(), nullable=True),
            sa.Column("hourly_rate_non_member_off_peak", sa.Integer(), nullable=True),
            sa.Column("hourly_rate_member_off_peak", sa.Integer(), nullable=True),
            sa.Column("facility_display_name", sa.Text(), nullable=True),
            sa.Column("peak_hours", JSONB(), nullable=True),
            sa.Column("location_display_name", sa.Text(), nullable=True),
            sa.Column("podplay_pod", sa.Text(), nullable=True),
            sa.Column("podplay_pod_id", sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            schema=schema,
        )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if _table_exists("organizations", schema):
        op.drop_table("organizations", schema=schema)
