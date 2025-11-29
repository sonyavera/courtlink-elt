"""refactor_facility_reviews_to_aggregate_data

Revision ID: refactor_facility_reviews
Revises: rename_metadata_column
Create Date: 2025-01-15 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = 'refactor_facility_reviews'
down_revision: Union[str, None] = 'rename_metadata_column'
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


def _column_exists(table_name: str, column_name: str, schema: str = None) -> bool:
    """Check if a column exists in a table."""
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

    if not _table_exists("facility_reviews", schema):
        # Table doesn't exist, create it with new structure
        op.create_table(
            "facility_reviews",
            sa.Column("id", sa.BigInteger(), nullable=False, autoincrement=True),
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("review_service", sa.Text(), nullable=False),
            sa.Column("num_reviews", sa.Integer(), nullable=True),
            sa.Column("avg_review", sa.Numeric(), nullable=True),
            sa.Column("link_to_reviews", sa.Text(), nullable=True),
            sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
            sa.PrimaryKeyConstraint("id"),
            schema=schema,
        )
        op.create_index(
            "facility_reviews_client_service_idx",
            "facility_reviews",
            ["client_code", "review_service"],
            unique=True,
            schema=schema,
        )
        return

    # Table exists, need to refactor it
    # First, clear all existing data since we're changing from individual reviews to aggregates
    op.execute(f'DELETE FROM "{schema}".facility_reviews')

    # Drop old index
    try:
        op.drop_index(
            "facility_reviews_client_provider_review_idx",
            table_name="facility_reviews",
            schema=schema,
        )
    except Exception:
        pass

    # Drop old columns
    old_columns = [
        "review_provider",
        "review_id",
        "author_name",
        "author_url",
        "profile_photo_url",
        "rating",
        "text",
        "time",
        "relative_time_description",
    ]
    for col in old_columns:
        if _column_exists("facility_reviews", col, schema):
            op.drop_column("facility_reviews", col, schema=schema)

    # Add new columns (data already cleared, so we can add as NOT NULL)
    if not _column_exists("facility_reviews", "review_service", schema):
        op.add_column(
            "facility_reviews",
            sa.Column("review_service", sa.Text(), nullable=False),
            schema=schema,
        )

    if not _column_exists("facility_reviews", "num_reviews", schema):
        op.add_column(
            "facility_reviews",
            sa.Column("num_reviews", sa.Integer(), nullable=True),
            schema=schema,
        )

    if not _column_exists("facility_reviews", "avg_review", schema):
        op.add_column(
            "facility_reviews",
            sa.Column("avg_review", sa.Numeric(), nullable=True),
            schema=schema,
        )

    if not _column_exists("facility_reviews", "link_to_reviews", schema):
        op.add_column(
            "facility_reviews",
            sa.Column("link_to_reviews", sa.Text(), nullable=True),
            schema=schema,
        )

    if not _column_exists("facility_reviews", "last_updated_at", schema):
        op.add_column(
            "facility_reviews",
            sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=True),
            schema=schema,
        )

    # Deduplicate any existing rows (in case migration was partially run)
    # Keep the most recent row for each client_code/review_service combination
    op.execute(
        f"""
        DELETE FROM "{schema}".facility_reviews a
        USING "{schema}".facility_reviews b
        WHERE a.id < b.id
        AND a.client_code = b.client_code
        AND a.review_service = b.review_service
        """
    )

    # Create new unique index (after deduplication)
    op.create_index(
        "facility_reviews_client_service_idx",
        "facility_reviews",
        ["client_code", "review_service"],
        unique=True,
        schema=schema,
    )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not _table_exists("facility_reviews", schema):
        return

    # Drop new index
    try:
        op.drop_index(
            "facility_reviews_client_service_idx",
            table_name="facility_reviews",
            schema=schema,
        )
    except Exception:
        pass

    # Drop new columns
    new_columns = [
        "review_service",
        "num_reviews",
        "avg_review",
        "link_to_reviews",
        "last_updated_at",
    ]
    for col in new_columns:
        if _column_exists("facility_reviews", col, schema):
            op.drop_column("facility_reviews", col, schema=schema)

    # Add back old columns
    op.add_column(
        "facility_reviews",
        sa.Column("review_provider", sa.Text(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("review_id", sa.Text(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("author_name", sa.Text(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("author_url", sa.Text(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("profile_photo_url", sa.Text(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("rating", sa.Integer(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("text", sa.Text(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("time", sa.DateTime(timezone=True), nullable=True),
        schema=schema,
    )
    op.add_column(
        "facility_reviews",
        sa.Column("relative_time_description", sa.Text(), nullable=True),
        schema=schema,
    )

    # Recreate old index
    op.create_index(
        "facility_reviews_client_provider_review_idx",
        "facility_reviews",
        ["client_code", "review_provider", "review_id"],
        unique=True,
        schema=schema,
    )

