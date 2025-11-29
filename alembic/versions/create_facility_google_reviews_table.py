"""create_facility_google_reviews_table

Revision ID: create_facility_google_reviews
Revises: create_facility_details
Create Date: 2025-11-26 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'create_facility_google_reviews'
down_revision: Union[str, None] = 'create_facility_details'
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

    if not _table_exists("facility_google_reviews", schema):
        op.create_table(
            "facility_google_reviews",
            sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("google_review_id", sa.Text(), nullable=False),
            sa.Column("author_name", sa.Text(), nullable=True),
            sa.Column("author_url", sa.Text(), nullable=True),
            sa.Column("profile_photo_url", sa.Text(), nullable=True),
            sa.Column("rating", sa.Integer(), nullable=True),
            sa.Column("text", sa.Text(), nullable=True),
            sa.Column("time", sa.DateTime(timezone=True), nullable=True),
            sa.Column("relative_time_description", sa.Text(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=True,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                onupdate=sa.func.now(),
                nullable=True,
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.Index(
                "facility_google_reviews_client_review_idx",
                "client_code",
                "google_review_id",
                unique=True,
            ),
            schema=schema,
        )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if _table_exists("facility_google_reviews", schema):
        op.drop_table("facility_google_reviews", schema=schema)

