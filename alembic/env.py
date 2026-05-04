"""Alembic migration environment.

Loads the database URL from application settings and uses the
SQLAlchemy Base metadata for autogeneration.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from lakehouse.config import get_settings
from lakehouse.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url with the real connection string (sync version),
# unless explicitly set via -x sqlalchemy.url=... or environment override.
if not config.get_main_option("sqlalchemy.url") or config.get_main_option("sqlalchemy.url") == "driver://user:pass@localhost/dbname":
    settings = get_settings()
    config.set_main_option("sqlalchemy.url", settings.sync_database_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — generates SQL without connecting."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
