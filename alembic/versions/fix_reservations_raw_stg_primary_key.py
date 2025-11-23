"""fix_reservations_raw_stg_primary_key_to_include_member_id

Revision ID: fix_reservations_stg_pk
Revises: ed57ca2b21dc
Create Date: 2025-01-14 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from dotenv import load_dotenv
from pathlib import Path
import os


# revision identifiers, used by Alembic.
revision: str = "fix_reservations_stg_pk"
down_revision: Union[str, None] = "ed57ca2b21dc"
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

    if not schema:
        print("Warning: PG_SCHEMA not set, skipping migration")
        return

    if _table_exists("reservations_raw_stg", schema):
        # Check if constraints/indexes already exist
        bind = op.get_bind()
        inspector = inspect(bind)

        # Check for existing primary key
        pk_constraint = inspector.get_pk_constraint(
            "reservations_raw_stg", schema=schema
        )
        has_old_pk = pk_constraint and set(pk_constraint["constrained_columns"]) == {
            "client_code",
            "reservation_id",
        }

        indexes = inspector.get_indexes("reservations_raw_stg", schema=schema)
        index_exists = any(
            idx["name"] == "reservations_raw_stg_client_res_member_idx"
            for idx in indexes
        )

        if has_old_pk:
            # Drop the old primary key constraint
            op.drop_constraint(
                "reservations_raw_stg_pkey",
                "reservations_raw_stg",
                schema=schema,
                type_="primary",
            )
            print(f"Dropped old primary key on {schema}.reservations_raw_stg")

        if not index_exists:
            # Add unique index on (client_code, reservation_id, member_id)
            # This allows multiple people with the same reservation_id
            # PostgreSQL treats each NULL as distinct, so multiple NULLs are allowed
            op.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS reservations_raw_stg_client_res_member_idx
                ON "{schema}"."reservations_raw_stg" (client_code, reservation_id, member_id)
                """
            )

            print(
                f"Added unique index on {schema}.reservations_raw_stg "
                f"for (client_code, reservation_id, member_id)"
            )
        else:
            print(f"Unique index already exists on {schema}.reservations_raw_stg")


def downgrade() -> None:
    # Get schema from environment
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    schema = os.getenv("PG_SCHEMA")

    if not schema:
        print("Warning: PG_SCHEMA not set, skipping migration")
        return

    if _table_exists("reservations_raw_stg", schema):
        # Drop the unique index
        op.execute(
            f'DROP INDEX IF EXISTS "{schema}".reservations_raw_stg_client_res_member_idx'
        )

        # Restore the original primary key
        op.create_primary_key(
            "reservations_raw_stg_pkey",
            "reservations_raw_stg",
            ["client_code", "reservation_id"],
            schema=schema,
        )

        print(
            f"Reverted {schema}.reservations_raw_stg to original primary key (client_code, reservation_id)"
        )
