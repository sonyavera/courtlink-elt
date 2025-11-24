"""add_event_timezone_to_facility_events

Revision ID: c5fa0f50c0a8
Revises: fix_reservations_stg_pk
Create Date: 2025-11-24 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = "c5fa0f50c0a8"
down_revision: Union[str, None] = "fix_reservations_stg_pk"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table_name: str, column_name: str, schema: str | None = None) -> bool:
    """Check if a column exists on the given table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        columns = [
            col["name"]
            for col in inspector.get_columns(table_name, schema=schema)
        ]
        return column_name in columns
    except Exception:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE {schema_filter}
                      table_name = :table_name
                  AND column_name = :column_name
            )
        """
        if schema:
            schema_filter = "table_schema = :schema AND"
        else:
            schema_filter = ""

        stmt = text(query.format(schema_filter=schema_filter))
        params = {"table_name": table_name, "column_name": column_name}
        if schema:
            params["schema"] = schema
        result = bind.execute(stmt, params)
        return result.scalar()


def upgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _column_exists("facility_events_raw", "event_timezone", schema):
        op.add_column(
            "facility_events_raw",
            sa.Column("event_timezone", sa.Text(), nullable=True),
        )


def downgrade() -> None:
    op.drop_column("facility_events_raw", "event_timezone")

