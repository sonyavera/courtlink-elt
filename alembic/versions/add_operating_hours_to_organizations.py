"""add_operating_hours_to_organizations

Revision ID: g9c4d6e82b25
Revises: f8b3c5d92a14
Create Date: 2025-11-27 20:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = "g9c4d6e82b25"
down_revision: Union[str, None] = "f8b3c5d92a14"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table_name: str, column_name: str, schema: str = None) -> bool:
    """Check if a column exists in a table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        columns = inspector.get_columns(table_name, schema=schema)
        return any(col["name"] == column_name for col in columns)
    except Exception:
        return False


def upgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _column_exists("organizations", "operating_hours", schema):
        op.add_column(
            "organizations",
            sa.Column("operating_hours", JSONB(), nullable=True),
            schema=schema,
        )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if _column_exists("organizations", "operating_hours", schema):
        op.drop_column("organizations", "operating_hours", schema=schema)

