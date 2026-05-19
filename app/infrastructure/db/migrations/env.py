"""
app/infrastructure/db/migrations/env.py — Alembic migration environment.

Configured for async SQLAlchemy (asyncpg driver). Supports both:
  - alembic upgrade head     (applies migrations)
  - alembic revision --autogenerate  (generates new migration from model diff)

All ORM models are imported via import_all_models() so autogenerate sees
the full schema.
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[4]))

from app.core.config import get_settings
from app.infrastructure.db.base import Base, import_all_models

# Import all models so autogenerate detects all tables.
import_all_models()

# Alembic Config object.
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url from application settings so we don't
# duplicate the connection string in alembic.ini.
config.set_main_option("sqlalchemy.url", str(get_settings().DATABASE_URL))

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations without a database connection (generates SQL script)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run migrations against a live async database connection."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
