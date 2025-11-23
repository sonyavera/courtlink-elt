"""add_court_name_and_period_type_to_court_availabilities

Revision ID: 9e56d762bfaa
Revises: 81a5137f51f6
Create Date: 2025-11-23 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = '9e56d762bfaa'
down_revision: Union[str, None] = '81a5137f51f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table_name: str, column_name: str, schema: str = None) -> bool:
    """Check if a column exists in the specified table."""
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

    if not _column_exists('facility_court_availabilities', 'court_name', schema):
        op.add_column('facility_court_availabilities', sa.Column('court_name', sa.Text(), nullable=True))
    if not _column_exists('facility_court_availabilities', 'period_type', schema):
        op.add_column('facility_court_availabilities', sa.Column('period_type', sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column('facility_court_availabilities', 'period_type')
    op.drop_column('facility_court_availabilities', 'court_name')
