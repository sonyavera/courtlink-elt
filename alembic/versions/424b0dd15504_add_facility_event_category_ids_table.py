"""add_facility_event_categories_table

Revision ID: 424b0dd15504
Revises: b55a096cc018
Create Date: 2025-11-23 10:17:19.965359

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '424b0dd15504'
down_revision: Union[str, None] = 'b55a096cc018'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Get schema from environment
    import os
    from dotenv import load_dotenv
    from pathlib import Path
    from sqlalchemy import inspect, text
    
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
    
    # Create facility_event_categories table
    if not _table_exists("facility_event_categories"):
        op.create_table(
            "facility_event_categories",
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("source_system", sa.Text(), nullable=False),
            sa.Column("id", sa.Text(), nullable=False),
            sa.Column("event_category_name", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.PrimaryKeyConstraint("client_code", "source_system", "id"),
        )


def downgrade() -> None:
    op.drop_table("facility_event_categories")

