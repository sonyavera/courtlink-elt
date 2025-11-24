"""remove_event_timezone_column

Revision ID: e4ac0c4d3f3a
Revises: c5fa0f50c0a8
Create Date: 2025-11-24 12:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = "e4ac0c4d3f3a"
down_revision: Union[str, None] = "c5fa0f50c0a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table_name: str, column_name: str, schema: str | None = None) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        cols = inspector.get_columns(table_name, schema=schema)
        return any(col["name"] == column_name for col in cols)
    except Exception:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE {schema_filter}
                      table_name = :table_name
                  AND column_name = :column_name
            )
        """
        schema_filter = "table_schema = :schema AND" if schema else ""
        stmt = text(query.format(schema_filter=schema_filter))
        params = {"table_name": table_name, "column_name": column_name}
        if schema:
            params["schema"] = schema
        result = bind.execute(stmt, params)
        return bool(result.scalar())


def upgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if _column_exists("facility_events_raw", "event_timezone", schema):
        op.drop_column("facility_events_raw", "event_timezone")


def downgrade() -> None:
    op.add_column(
        "facility_events_raw",
        sa.Column("event_timezone", sa.Text(), nullable=True),
    )

