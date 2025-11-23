"""Update reservations

Revision ID: 002_update_reservations
Revises: 001_initial
Create Date: 2024-01-02 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

# revision identifiers, used by Alembic.
revision: str = "002_update_reservations"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table_name: str, column_name: str, schema: str = None) -> bool:
    """Check if a column exists in a table."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        if schema:
            columns = [
                col["name"] for col in inspector.get_columns(table_name, schema=schema)
            ]
        else:
            columns = [col["name"] for col in inspector.get_columns(table_name)]
        return column_name in columns
    except Exception:
        # Fallback to direct SQL query
        try:
            if schema:
                query = text(
                    f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_schema = :schema 
                        AND table_name = :table_name
                        AND column_name = :column_name
                    )
                """
                )
                result = bind.execute(
                    query,
                    {
                        "schema": schema,
                        "table_name": table_name,
                        "column_name": column_name,
                    },
                )
            else:
                query = text(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = :table_name
                        AND column_name = :column_name
                    )
                """
                )
                result = bind.execute(
                    query, {"table_name": table_name, "column_name": column_name}
                )
            return result.scalar()
        except Exception:
            return False


def upgrade() -> None:
    # Get schema from environment
    import os
    from dotenv import load_dotenv
    from pathlib import Path

    # Load .env file
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)

    schema = os.getenv("PG_SCHEMA")

    # Add client_code and source_system to reservation_cancellations_raw
    if not _column_exists("reservation_cancellations_raw", "client_code", schema):
        op.add_column(
            "reservation_cancellations_raw",
            sa.Column("client_code", sa.Text(), nullable=True),
        )
    if not _column_exists("reservation_cancellations_raw", "source_system", schema):
        op.add_column(
            "reservation_cancellations_raw",
            sa.Column("source_system", sa.Text(), nullable=True),
        )

    # Add client_code and source_system to reservation_cancellations_raw_stg
    if not _column_exists("reservation_cancellations_raw_stg", "client_code", schema):
        op.add_column(
            "reservation_cancellations_raw_stg",
            sa.Column("client_code", sa.Text(), nullable=True),
        )
    if not _column_exists("reservation_cancellations_raw_stg", "source_system", schema):
        op.add_column(
            "reservation_cancellations_raw_stg",
            sa.Column("source_system", sa.Text(), nullable=True),
        )

    # Add reservation_cancelled_at to reservations_raw
    if not _column_exists("reservations_raw", "reservation_cancelled_at", schema):
        op.add_column(
            "reservations_raw",
            sa.Column(
                "reservation_cancelled_at", sa.DateTime(timezone=True), nullable=True
            ),
        )

    # Add reservation_cancelled_at to reservations_raw_stg
    if not _column_exists("reservations_raw_stg", "reservation_cancelled_at", schema):
        op.add_column(
            "reservations_raw_stg",
            sa.Column(
                "reservation_cancelled_at", sa.DateTime(timezone=True), nullable=True
            ),
        )

    # Add id column to reservation_cancellations_raw if it doesn't exist
    # This is needed because Alembic requires primary keys, and the original tables didn't have one
    if not _column_exists("reservation_cancellations_raw", "id", schema):
        # Add the column as nullable first
        op.add_column(
            "reservation_cancellations_raw",
            sa.Column("id", sa.Integer(), nullable=True),
        )
        # Populate it with a sequence
        bind = op.get_bind()
        bind.execute(
            text(
                f"""
            CREATE SEQUENCE IF NOT EXISTS {schema or 'public'}.reservation_cancellations_raw_id_seq;
            ALTER TABLE {schema or 'public'}.reservation_cancellations_raw 
            ALTER COLUMN id SET DEFAULT nextval('{schema or 'public'}.reservation_cancellations_raw_id_seq');
            UPDATE {schema or 'public'}.reservation_cancellations_raw 
            SET id = nextval('{schema or 'public'}.reservation_cancellations_raw_id_seq')
            WHERE id IS NULL;
            ALTER TABLE {schema or 'public'}.reservation_cancellations_raw 
            ALTER COLUMN id SET NOT NULL;
            ALTER SEQUENCE {schema or 'public'}.reservation_cancellations_raw_id_seq OWNED BY {schema or 'public'}.reservation_cancellations_raw.id;
        """
            )
        )
        # Make it the primary key
        op.create_primary_key(
            "reservation_cancellations_raw_pkey",
            "reservation_cancellations_raw",
            ["id"],
        )

    # Add id column to reservation_cancellations_raw_stg if it doesn't exist
    if not _column_exists("reservation_cancellations_raw_stg", "id", schema):
        op.add_column(
            "reservation_cancellations_raw_stg",
            sa.Column("id", sa.Integer(), nullable=True),
        )
        bind = op.get_bind()
        bind.execute(
            text(
                f"""
            CREATE SEQUENCE IF NOT EXISTS {schema or 'public'}.reservation_cancellations_raw_stg_id_seq;
            ALTER TABLE {schema or 'public'}.reservation_cancellations_raw_stg 
            ALTER COLUMN id SET DEFAULT nextval('{schema or 'public'}.reservation_cancellations_raw_stg_id_seq');
            UPDATE {schema or 'public'}.reservation_cancellations_raw_stg 
            SET id = nextval('{schema or 'public'}.reservation_cancellations_raw_stg_id_seq')
            WHERE id IS NULL;
            ALTER TABLE {schema or 'public'}.reservation_cancellations_raw_stg 
            ALTER COLUMN id SET NOT NULL;
            ALTER SEQUENCE {schema or 'public'}.reservation_cancellations_raw_stg_id_seq OWNED BY {schema or 'public'}.reservation_cancellations_raw_stg.id;
        """
            )
        )
        op.create_primary_key(
            "reservation_cancellations_raw_stg_pkey",
            "reservation_cancellations_raw_stg",
            ["id"],
        )

    # Note: The data backfill logic from the original SQL migration would need to be run separately
    # as Alembic migrations are typically for schema changes only. You may want to create
    # a separate data migration script for the UPDATE statements.


def downgrade() -> None:
    op.drop_column("reservations_raw_stg", "reservation_cancelled_at")
    op.drop_column("reservations_raw", "reservation_cancelled_at")
    op.drop_column("reservation_cancellations_raw_stg", "source_system")
    op.drop_column("reservation_cancellations_raw_stg", "client_code")
    op.drop_column("reservation_cancellations_raw", "source_system")
    op.drop_column("reservation_cancellations_raw", "client_code")
