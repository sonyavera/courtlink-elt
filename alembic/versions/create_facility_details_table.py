"""create_facility_details_table

Revision ID: create_facility_details
Revises: add_description_stg
Create Date: 2025-11-26 14:00:00.000000

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
revision: str = 'create_facility_details'
down_revision: Union[str, None] = 'add_description_stg'
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

    if not _table_exists("facility_details", schema):
        op.create_table(
            "facility_details",
            sa.Column("client_code", sa.Text(), nullable=False),
            sa.Column("street_address", sa.Text(), nullable=True),
            sa.Column("city", sa.Text(), nullable=True),
            sa.Column("state", sa.Text(), nullable=True),
            sa.Column("zip_code", sa.Text(), nullable=True),
            sa.Column("country", sa.Text(), nullable=True),
            sa.Column("latitude", sa.Numeric(), nullable=True),
            sa.Column("longitude", sa.Numeric(), nullable=True),
            sa.Column("full_address", sa.Text(), nullable=True),
            sa.Column("number_of_courts", sa.Integer(), nullable=True),
            sa.Column("primary_court_type", sa.Text(), nullable=True),
            sa.Column("court_types", JSONB(), nullable=True),
            sa.Column("indoor_outdoor", sa.Text(), nullable=True),
            sa.Column("court_surface_type", sa.Text(), nullable=True),
            sa.Column("has_showers", sa.Boolean(), nullable=True),
            sa.Column("has_lounge_area", sa.Boolean(), nullable=True),
            sa.Column("has_paddle_rentals", sa.Boolean(), nullable=True),
            sa.Column("has_pro_shop", sa.Boolean(), nullable=True),
            sa.Column("has_food_service", sa.Boolean(), nullable=True),
            sa.Column("has_parking", sa.Boolean(), nullable=True),
            sa.Column("parking_type", sa.Text(), nullable=True),
            sa.Column("has_wifi", sa.Boolean(), nullable=True),
            sa.Column("has_locker_rooms", sa.Boolean(), nullable=True),
            sa.Column("has_water_fountains", sa.Boolean(), nullable=True),
            sa.Column("is_autonomous_facility", sa.Boolean(), nullable=True),
            sa.Column("facility_type", sa.Text(), nullable=True),
            sa.Column("ownership_type", sa.Text(), nullable=True),
            sa.Column("year_opened", sa.Integer(), nullable=True),
            sa.Column("facility_size_sqft", sa.Integer(), nullable=True),
            sa.Column("google_place_id", sa.Text(), nullable=True),
            sa.Column("last_review_sync_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("amenities_list", sa.Text(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("facility_metadata", JSONB(), nullable=True),
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
            sa.PrimaryKeyConstraint("client_code"),
            schema=schema,
        )


def downgrade() -> None:
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if _table_exists("facility_details", schema):
        op.drop_table("facility_details", schema=schema)

