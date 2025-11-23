"""add_admission_rates_to_facility_events

Revision ID: 5daaa8576dce
Revises: bef652cf4258
Create Date: 2025-11-23 11:36:13.467536

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = '5daaa8576dce'
down_revision: Union[str, None] = 'bef652cf4258'
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
        # Fallback to direct SQL query
        try:
            if schema:
                query = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_schema = :schema
                        AND table_name = :table_name
                        AND column_name = :column_name
                    )
                """)
                result = bind.execute(query, {
                    "schema": schema,
                    "table_name": table_name,
                    "column_name": column_name
                })
            else:
                query = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns
                        WHERE table_name = :table_name
                        AND column_name = :column_name
                    )
                """)
                result = bind.execute(query, {
                    "table_name": table_name,
                    "column_name": column_name
                })
            return result.scalar()
        except Exception:
            return False


def upgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _column_exists('facility_events_raw', 'admission_rate_regular', schema):
        op.add_column('facility_events_raw', sa.Column('admission_rate_regular', sa.Numeric(), nullable=True))
    if not _column_exists('facility_events_raw', 'admission_rate_member', schema):
        op.add_column('facility_events_raw', sa.Column('admission_rate_member', sa.Numeric(), nullable=True))


def downgrade() -> None:
    op.drop_column('facility_events_raw', 'admission_rate_member')
    op.drop_column('facility_events_raw', 'admission_rate_regular')
