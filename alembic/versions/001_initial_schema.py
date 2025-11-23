"""Initial schema

Revision ID: 001_initial
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str, schema: str = None) -> bool:
    """Check if a table exists in the database."""
    bind = op.get_bind()
    inspector = inspect(bind)
    try:
        if schema:
            tables = inspector.get_table_names(schema=schema)
        else:
            # Check all schemas
            schemas = inspector.get_schema_names()
            for sch in schemas:
                if table_name in inspector.get_table_names(schema=sch):
                    return True
            return False
        return table_name in tables
    except Exception:
        # If inspection fails, try a direct query
        try:
            if schema:
                query = text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = :schema 
                        AND table_name = :table_name
                    )
                """)
                result = bind.execute(query, {"schema": schema, "table_name": table_name})
            else:
                query = text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = :table_name
                    )
                """)
                result = bind.execute(query, {"table_name": table_name})
            return result.scalar()
        except Exception:
            return False


def upgrade() -> None:
    # Get schema from environment or connection
    import os
    from dotenv import load_dotenv
    from pathlib import Path
    
    # Load .env file
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    schema = os.getenv("PG_SCHEMA")
    
    # If not in env, try to get from connection
    if not schema:
        bind = op.get_bind()
        try:
            result = bind.execute(text("SHOW search_path"))
            search_path = result.scalar()
            if search_path and '"' in search_path:
                # Extract schema from search_path if it's quoted
                import re
                match = re.search(r'"([^"]+)"', search_path)
                if match:
                    schema = match.group(1)
        except:
            pass
    
    # ELT watermarks
    if not _table_exists('elt_watermarks', schema):
        op.create_table(
            'elt_watermarks',
            sa.Column('source_name', sa.Text(), nullable=False),
            sa.Column('last_loaded_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('last_record_created_at', sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint('source_name')
        )
        op.create_index(
            'elt_watermarks_source_name_idx',
            'elt_watermarks',
            ['source_name'],
            unique=True
        )

    # Members raw
    if not _table_exists('members_raw', schema):
        op.create_table(
            'members_raw',
            sa.Column('client_code', sa.Text(), nullable=False),
            sa.Column('member_id', sa.Text(), nullable=False),
            sa.PrimaryKeyConstraint('client_code', 'member_id'),
            sa.Column('first_name', sa.Text(), nullable=True),
            sa.Column('last_name', sa.Text(), nullable=True),
            sa.Column('gender', sa.Text(), nullable=True),
            sa.Column('date_of_birth', sa.Date(), nullable=True),
            sa.Column('phone_number', sa.Text(), nullable=True),
            sa.Column('email', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        )
        op.create_index(
            'members_raw_client_member_idx',
            'members_raw',
            ['client_code', 'member_id'],
            unique=True
        )

    # Members raw staging
    if not _table_exists('members_raw_stg', schema):
        op.create_table(
            'members_raw_stg',
            sa.Column('client_code', sa.Text(), nullable=False),
            sa.Column('member_id', sa.Text(), nullable=False),
            sa.PrimaryKeyConstraint('client_code', 'member_id'),
            sa.Column('first_name', sa.Text(), nullable=True),
            sa.Column('last_name', sa.Text(), nullable=True),
            sa.Column('gender', sa.Text(), nullable=True),
            sa.Column('date_of_birth', sa.Date(), nullable=True),
            sa.Column('phone_number', sa.Text(), nullable=True),
            sa.Column('club_member_key', sa.Text(), nullable=True),
            sa.Column('email', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        )
        op.create_index(
            'members_raw_stg_client_member_idx',
            'members_raw_stg',
            ['client_code', 'member_id'],
            unique=True
        )

    # Reservations raw
    if not _table_exists('reservations_raw', schema):
        op.create_table(
            'reservations_raw',
            sa.Column('client_code', sa.Text(), nullable=False),
            sa.Column('reservation_id', sa.Text(), nullable=False),
            sa.Column('event_id', sa.Text(), nullable=True),
            sa.Column('member_id', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('client_code', 'reservation_id'),
            sa.Column('reservation_created_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_updated_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_start_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_end_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        )
        op.create_index(
            'reservations_raw_client_res_member_idx',
            'reservations_raw',
            ['client_code', 'reservation_id', 'member_id'],
            unique=True
        )

    # Reservations raw staging
    if not _table_exists('reservations_raw_stg', schema):
        op.create_table(
            'reservations_raw_stg',
            sa.Column('client_code', sa.Text(), nullable=False),
            sa.Column('reservation_id', sa.Text(), nullable=False),
            sa.Column('event_id', sa.Text(), nullable=True),
            sa.Column('member_id', sa.Text(), nullable=True),
            sa.PrimaryKeyConstraint('client_code', 'reservation_id'),
            sa.Column('reservation_created_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_updated_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_start_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_end_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        )

    # Reservation cancellations raw
    if not _table_exists('reservation_cancellations_raw', schema):
        op.create_table(
            'reservation_cancellations_raw',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('event_id', sa.Text(), nullable=True),
            sa.Column('reservation_id', sa.Text(), nullable=True),
            sa.Column('reservation_type', sa.Text(), nullable=True),
            sa.Column('reservation_created_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_start_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_end_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('cancelled_on', sa.DateTime(timezone=True), nullable=True),
            sa.Column('day_of_week', sa.Text(), nullable=True),
            sa.Column('is_program', sa.Boolean(), nullable=True),
            sa.Column('program_name', sa.Text(), nullable=True),
            sa.Column('player_name', sa.Text(), nullable=True),
            sa.Column('player_first_name', sa.Text(), nullable=True),
            sa.Column('player_last_name', sa.Text(), nullable=True),
            sa.Column('player_email', sa.Text(), nullable=True),
            sa.Column('player_phone', sa.Text(), nullable=True),
            sa.Column('fee', sa.Numeric(), nullable=True),
            sa.Column('is_team_event', sa.Boolean(), nullable=True),
            sa.Column('event_category_name', sa.Text(), nullable=True),
            sa.Column('event_category_id', sa.Text(), nullable=True),
            sa.Column('member_id', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
        )

    # Reservation cancellations raw staging
    if not _table_exists('reservation_cancellations_raw_stg', schema):
        op.create_table(
            'reservation_cancellations_raw_stg',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('event_id', sa.Text(), nullable=True),
            sa.Column('reservation_id', sa.Text(), nullable=True),
            sa.Column('reservation_type', sa.Text(), nullable=True),
            sa.Column('reservation_created_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_start_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reservation_end_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('cancelled_on', sa.DateTime(timezone=True), nullable=True),
            sa.Column('day_of_week', sa.Text(), nullable=True),
            sa.Column('is_program', sa.Boolean(), nullable=True),
            sa.Column('program_name', sa.Text(), nullable=True),
            sa.Column('player_name', sa.Text(), nullable=True),
            sa.Column('player_first_name', sa.Text(), nullable=True),
            sa.Column('player_last_name', sa.Text(), nullable=True),
            sa.Column('player_email', sa.Text(), nullable=True),
            sa.Column('player_phone', sa.Text(), nullable=True),
            sa.Column('fee', sa.Numeric(), nullable=True),
            sa.Column('is_team_event', sa.Boolean(), nullable=True),
            sa.Column('event_category_name', sa.Text(), nullable=True),
            sa.Column('event_category_id', sa.Text(), nullable=True),
            sa.Column('member_id', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
        )


def downgrade() -> None:
    op.drop_table('reservation_cancellations_raw_stg')
    op.drop_table('reservation_cancellations_raw')
    op.drop_index('reservations_raw_client_res_member_idx', table_name='reservations_raw')
    op.drop_table('reservations_raw_stg')
    op.drop_table('reservations_raw')
    op.drop_index('members_raw_stg_client_member_idx', table_name='members_raw_stg')
    op.drop_table('members_raw_stg')
    op.drop_index('members_raw_client_member_idx', table_name='members_raw')
    op.drop_table('members_raw')
    op.drop_index('elt_watermarks_source_name_idx', table_name='elt_watermarks')
    op.drop_table('elt_watermarks')
