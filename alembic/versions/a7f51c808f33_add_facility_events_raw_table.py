"""add_facility_events_raw_table

Revision ID: a7f51c808f33
Revises: 0a51c0d82319
Create Date: 2025-11-23 10:58:51.311321

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a7f51c808f33'
down_revision: Union[str, None] = '0a51c0d82319'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Get schema from environment
    import os
    from dotenv import load_dotenv
    from pathlib import Path
    from sqlalchemy import inspect
    
    # Load .env file
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    schema = os.getenv("PG_SCHEMA")
    bind = op.get_bind()
    
    def _table_exists(table_name: str) -> bool:
        """Check if a table exists in the database."""
        inspector = inspect(bind)
        try:
            if schema:
                return table_name in inspector.get_table_names(schema=schema)
            return table_name in inspector.get_table_names()
        except Exception:
            return False
    
    # Create facility_events_raw table if it doesn't exist
    if not _table_exists("facility_events_raw"):
        op.create_table(
            "facility_events_raw",
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("event_id", sa.Text(), nullable=False),
            sa.Column("event_name", sa.Text(), nullable=True),
            sa.Column("event_type", sa.Text(), nullable=True),
            sa.Column("event_start_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("event_end_time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.PrimaryKeyConstraint("client_code", "source_system", "event_id"),
        )


def downgrade() -> None:
    op.drop_table("facility_events_raw")

