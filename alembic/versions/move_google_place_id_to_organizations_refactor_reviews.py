"""move_google_place_id_to_organizations_refactor_reviews

Revision ID: move_google_place_id_refactor_reviews
Revises: update_facility_details
Create Date: 2025-01-15 12:00:00.000000

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
revision: str = 'move_google_place_id'
down_revision: Union[str, None] = 'update_facility_details'
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

    # 1. Add google_place_id to organizations table
    if _table_exists("organizations", schema):
        if not _column_exists("organizations", "google_place_id", schema):
            op.add_column(
                "organizations",
                sa.Column("google_place_id", sa.Text(), nullable=True),
                schema=schema,
            )

    # 2. Migrate google_place_id from facility_details to organizations
    if _table_exists("facility_details", schema) and _table_exists("organizations", schema):
        if _column_exists("facility_details", "google_place_id", schema):
            # Copy google_place_id from facility_details to organizations
            op.execute(
                f"""
                UPDATE "{schema}".organizations o
                SET google_place_id = fd.google_place_id
                FROM "{schema}".facility_details fd
                WHERE o.client_code = fd.client_code
                AND fd.google_place_id IS NOT NULL
                """
            )

    # 3. Remove google_place_id and last_review_sync_at from facility_details
    if _table_exists("facility_details", schema):
        if _column_exists("facility_details", "google_place_id", schema):
            op.drop_column("facility_details", "google_place_id", schema=schema)
        if _column_exists("facility_details", "last_review_sync_at", schema):
            op.drop_column("facility_details", "last_review_sync_at", schema=schema)

    # 4. Rename facility_google_reviews to facility_reviews and add review_provider
    if _table_exists("facility_google_reviews", schema):
        # Rename the table
        op.rename_table("facility_google_reviews", "facility_reviews", schema=schema)
        
        # Rename google_review_id to review_id
        if _column_exists("facility_reviews", "google_review_id", schema):
            op.alter_column(
                "facility_reviews",
                "google_review_id",
                new_column_name="review_id",
                schema=schema,
            )
        
        # Add review_provider column and set all existing rows to 'google'
        if not _column_exists("facility_reviews", "review_provider", schema):
            op.add_column(
                "facility_reviews",
                sa.Column("review_provider", sa.Text(), nullable=True),
                schema=schema,
            )
            # Set all existing reviews to 'google'
            op.execute(
                f'UPDATE "{schema}".facility_reviews SET review_provider = \'google\' WHERE review_provider IS NULL'
            )
            # Make it NOT NULL after setting values
            op.alter_column(
                "facility_reviews",
                "review_provider",
                nullable=False,
                schema=schema,
            )
        
        # Drop old unique index and create new one
        try:
            op.drop_index(
                "facility_google_reviews_client_review_idx",
                table_name="facility_reviews",
                schema=schema,
            )
        except Exception:
            pass  # Index might not exist or have different name
        
        # Create new unique index on (client_code, review_provider, review_id)
        op.create_index(
            "facility_reviews_client_provider_review_idx",
            "facility_reviews",
            ["client_code", "review_provider", "review_id"],
            unique=True,
            schema=schema,
        )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    # Reverse the changes
    if _table_exists("facility_reviews", schema):
        # Drop new index
        try:
            op.drop_index(
                "facility_reviews_client_provider_review_idx",
                table_name="facility_reviews",
                schema=schema,
            )
        except Exception:
            pass
        
        # Remove review_provider column
        if _column_exists("facility_reviews", "review_provider", schema):
            op.drop_column("facility_reviews", "review_provider", schema=schema)
        
        # Rename review_id back to google_review_id
        if _column_exists("facility_reviews", "review_id", schema):
            op.alter_column(
                "facility_reviews",
                "review_id",
                new_column_name="google_review_id",
                schema=schema,
            )
        
        # Rename table back
        op.rename_table("facility_reviews", "facility_google_reviews", schema=schema)
        
        # Recreate old index
        op.create_index(
            "facility_google_reviews_client_review_idx",
            "facility_google_reviews",
            ["client_code", "google_review_id"],
            unique=True,
            schema=schema,
        )

    # Add back columns to facility_details
    if _table_exists("facility_details", schema):
        if not _column_exists("facility_details", "google_place_id", schema):
            op.add_column(
                "facility_details",
                sa.Column("google_place_id", sa.Text(), nullable=True),
                schema=schema,
            )
        if not _column_exists("facility_details", "last_review_sync_at", schema):
            op.add_column(
                "facility_details",
                sa.Column("last_review_sync_at", sa.DateTime(timezone=True), nullable=True),
                schema=schema,
            )
        
        # Migrate google_place_id back from organizations to facility_details
        if _table_exists("organizations", schema):
            op.execute(
                f"""
                UPDATE "{schema}".facility_details fd
                SET google_place_id = o.google_place_id
                FROM "{schema}".organizations o
                WHERE o.client_code = fd.client_code
                AND o.google_place_id IS NOT NULL
                """
            )

    # Remove google_place_id from organizations
    if _table_exists("organizations", schema):
        if _column_exists("organizations", "google_place_id", schema):
            op.drop_column("organizations", "google_place_id", schema=schema)

