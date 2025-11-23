"""add_id_to_cancellation_tables

Revision ID: b55a096cc018
Revises: 002_update_reservations
Create Date: 2025-11-23 10:07:11.545729

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b55a096cc018'
down_revision: Union[str, None] = '002_update_reservations'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Get schema from environment
    import os
    from dotenv import load_dotenv
    from pathlib import Path
    from sqlalchemy import inspect, text
    
    # Load .env file
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    schema = os.getenv("PG_SCHEMA")
    bind = op.get_bind()
    
    def _column_exists(table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        inspector = inspect(bind)
        try:
            if schema:
                columns = [col["name"] for col in inspector.get_columns(table_name, schema=schema)]
            else:
                columns = [col["name"] for col in inspector.get_columns(table_name)]
            return column_name in columns
        except Exception:
            return False
    
    # Add id column to reservation_cancellations_raw if it doesn't exist
    if not _column_exists("reservation_cancellations_raw", "id"):
        # Add the column as nullable first
        op.add_column(
            "reservation_cancellations_raw",
            sa.Column("id", sa.Integer(), nullable=True),
        )
        # Populate it with a sequence
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
        # Make it the primary key (check if PK already exists first)
        try:
            op.create_primary_key(
                "reservation_cancellations_raw_pkey",
                "reservation_cancellations_raw",
                ["id"],
            )
        except Exception:
            # Primary key might already exist
            pass

    # Add id column to reservation_cancellations_raw_stg if it doesn't exist
    if not _column_exists("reservation_cancellations_raw_stg", "id"):
        op.add_column(
            "reservation_cancellations_raw_stg",
            sa.Column("id", sa.Integer(), nullable=True),
        )
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
        try:
            op.create_primary_key(
                "reservation_cancellations_raw_stg_pkey",
                "reservation_cancellations_raw_stg",
                ["id"],
            )
        except Exception:
            # Primary key might already exist
            pass


def downgrade() -> None:
    # Remove id columns (but this is optional - you might want to keep them)
    # op.drop_column("reservation_cancellations_raw_stg", "id")
    # op.drop_column("reservation_cancellations_raw", "id")
    pass

