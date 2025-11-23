from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool, text

from alembic import context

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import your models here
from migrations.models import Base
target_metadata = Base.metadata

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Parse PG_DSN to get connection parameters
pg_dsn = os.getenv("PG_DSN")
if pg_dsn:
    from urllib.parse import urlparse
    parsed = urlparse(pg_dsn)
    config.set_main_option("sqlalchemy.url", pg_dsn)
else:
    # Fallback to individual connection parameters
    db_host = os.getenv("DB_HOST", "localhost")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    
    sqlalchemy_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    config.set_main_option("sqlalchemy.url", sqlalchemy_url)

# Get schema from environment
pg_schema = os.getenv("PG_SCHEMA")
if not pg_schema:
    raise RuntimeError("PG_SCHEMA environment variable must be set")

# Set schema on all models
if pg_schema:
    for table in Base.metadata.tables.values():
        table.schema = pg_schema

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=pg_schema,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        # Set the schema search path
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"'))
        connection.execute(text(f'SET search_path TO "{pg_schema}"'))
        connection.commit()
        
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=pg_schema,
            include_schemas=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

