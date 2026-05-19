"""
app/infrastructure/db/session.py — Async database engine and session factory.

Refactored: zero global mutable state. All state is encapsulated inside the
_SessionFactoryRegistry singleton, accessed only through configure_session_factory /
get_session_factory / reset_session_factory. The old module-level
AsyncSessionLocal global is gone; every DB consumer receives a factory via
injection.

Isolation level design:
  Engine defaults to READ COMMITTED for all non-ledger queries.
  get_ledger_session_factory() returns a factory bound to a SERIALIZABLE
  execution_options overlay, scoped to ledger write sessions only.
"""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal registry — NOT exported; touched only through the three public
# functions below.
# ---------------------------------------------------------------------------


class _SessionFactoryRegistry:
    __slots__ = ("_engine", "_factory")

    def __init__(self) -> None:
        self._engine: Optional[AsyncEngine] = None
        self._factory: Optional[async_sessionmaker[AsyncSession]] = None

    # -- write -----------------------------------------------------------------

    def configure(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        self._engine = engine
        self._factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
        return self._factory

    async def dispose(self) -> None:
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._factory = None
            logger.info("Database engine disposed")

    def reset(self) -> None:
        """Hard reset — synchronous, for use in tests."""
        self._engine = None
        self._factory = None

    # -- read ------------------------------------------------------------------

    def get(self) -> async_sessionmaker[AsyncSession]:
        if self._factory is not None:
            return self._factory
        raise RuntimeError(
            "Session factory not configured. "
            "Call configure_session_factory() (or init_db() at startup) first."
        )

    def get_engine(self) -> AsyncEngine:
        if self._engine is not None:
            return self._engine
        raise RuntimeError("Database engine not initialised. Call init_db() first.")


_registry = _SessionFactoryRegistry()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def configure_session_factory(
    *,
    database_url: Optional[str] = None,
    pool_size: int = 10,
    max_overflow: int = 20,
    pool_timeout: int = 30,
    echo: bool = False,
) -> async_sessionmaker[AsyncSession]:
    """
    Create and register the engine + session factory.

    Accepts explicit parameters so tests can pass an in-process PostgreSQL URL
    without touching the application Settings object.  When called with no
    arguments it reads from get_settings().
    """
    settings = get_settings()
    url = database_url or str(settings.DATABASE_URL)

    engine = create_async_engine(
        url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,
        echo=echo,
        execution_options={"isolation_level": "READ COMMITTED"},
    )
    factory = _registry.configure(engine)
    logger.info(
        "Database engine initialised",
        extra={"database_url": url.split("@")[-1]},
    )
    return factory


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the currently registered session factory. Raises if not configured."""
    return _registry.get()


def reset_session_factory() -> None:
    """
    Tear down and unregister the session factory without async disposal.

    For use in tests where the engine was created externally (e.g. pytest-
    asyncio fixtures) and will be disposed by the fixture itself.
    """
    _registry.reset()


def get_ledger_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Session factory for ledger write operations (SERIALIZABLE isolation).

    Every call constructs a new async_sessionmaker bound to a SERIALIZABLE
    execution_options overlay on the shared engine.  The factory is cheap
    to construct; sessions are the expensive resource.
    """
    engine = _registry.get_engine()
    return async_sessionmaker(
        bind=engine.execution_options(isolation_level="SERIALIZABLE"),
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


# ---------------------------------------------------------------------------
# Lifecycle helpers (called from lifespan context manager)
# ---------------------------------------------------------------------------


async def init_db() -> None:
    """Initialise engine from application settings. Called once at startup."""
    settings = get_settings()
    configure_session_factory(
        database_url=str(settings.DATABASE_URL),
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_timeout=settings.DB_POOL_TIMEOUT,
        echo=settings.DEBUG,
    )


async def close_db() -> None:
    """Dispose the engine connection pool. Called once at shutdown."""
    await _registry.dispose()


async def get_engine() -> AsyncEngine:
    return _registry.get_engine()
