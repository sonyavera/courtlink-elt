"""add_registrant_counts_to_facility_events

Revision ID: bef652cf4258
Revises: a7f51c808f33
Create Date: 2025-11-23 11:13:16.888087

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bef652cf4258'
down_revision: Union[str, None] = 'a7f51c808f33'
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
    
    def _column_exists(table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        inspector = inspect(bind)
        try:
            if schema:
                columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
            else:
                columns = [col["name"] for col in inspector.get_columns(table_name)]
            return column_name in columns
        except Exception:
            return False
    
    # Add num_registrants column if it doesn't exist
    if not _column_exists("facility_events_raw", "num_registrants"):
        op.add_column(
            "facility_events_raw",
            sa.Column("num_registrants", sa.Integer(), nullable=True),
        )
    
    # Add max_registrants column if it doesn't exist
    if not _column_exists("facility_events_raw", "max_registrants"):
        op.add_column(
            "facility_events_raw",
            sa.Column("max_registrants", sa.Integer(), nullable=True),
        )


def downgrade() -> None:
    op.drop_column("facility_events_raw", "max_registrants")
    op.drop_column("facility_events_raw", "num_registrants")

