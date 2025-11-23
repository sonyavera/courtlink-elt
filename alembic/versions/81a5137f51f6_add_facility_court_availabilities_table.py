"""add_facility_court_availabilities_table

Revision ID: 81a5137f51f6
Revises: d6a2ef583018
Create Date: 2025-11-23 17:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = '81a5137f51f6'
down_revision: Union[str, None] = 'd6a2ef583018'
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
    bind = op.get_bind()

    if not _table_exists("facility_court_availabilities", schema):
        op.create_table(
            "facility_court_availabilities",
            sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("court_id", sa.Text(), nullable=False),
            sa.Column("slot_start", sa.DateTime(timezone=True), nullable=False),
            sa.Column("slot_end", sa.DateTime(timezone=True), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )


def downgrade() -> None:
    op.drop_table("facility_court_availabilities")
