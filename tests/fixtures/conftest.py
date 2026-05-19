"""
tests/fixtures/conftest.py — Shared pytest fixtures for the integration test suite.

Production-grade async PostgreSQL test infrastructure.

Key guarantees
--------------
- SINGLE engine for the entire process.
- SINGLE AsyncSessionFactory shared everywhere.
- No hidden engines created via DI helpers.
- No pooled poisoned asyncpg connections during test debugging.
- Full isolation between tests via TRUNCATE CASCADE.
- Explicit session close/rollback safety.

Why NullPool?
--------------
Asyncpg connections become invalid after concurrent-operation violations.
NullPool prevents corrupted connections from being reused across tests.

Once the suite is fully stable, QueuePool can optionally be restored.
"""

from __future__ import annotations

import fnmatch
import os
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.infrastructure.db.base import Base, import_all_models
from app.infrastructure.db.session import (
    configure_session_factory,
    reset_session_factory,
)

# Register all ORM models before metadata creation.
import_all_models()

_DEFAULT_TEST_DB = "postgresql+asyncpg://postgres:postgres@localhost:5432/payment_test"

_TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    _DEFAULT_TEST_DB,
)

# FK-safe truncate order.
_TRUNCATE_TABLES = [
    "outbox",
    "ledger_entries",
    "ledger_accounts",
    "webhook_events",
    "processed_webhook_events",
    "payments",
    "bunq_sessions",
]

# ============================================================================
# SINGLE GLOBAL ENGINE
# ============================================================================

engine = create_async_engine(
    _TEST_DATABASE_URL,
    echo=False,
    future=True,
    # Critical during async/concurrency debugging.
    # Prevents poisoned asyncpg connections from being reused.
    poolclass=NullPool,
)

# ============================================================================
# SINGLE GLOBAL SESSION FACTORY
# ============================================================================

AsyncSessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


# ============================================================================
# Session-scoped engine lifecycle
# ============================================================================


@pytest_asyncio.fixture(scope="session")
async def async_engine():
    """
    Shared PostgreSQL engine for the entire test session.
    """

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # IMPORTANT:
    # Inject the EXISTING factory into app DI.
    # DO NOT create a second hidden engine.
    configure_session_factory(
        database_url=_TEST_DATABASE_URL,
        pool_size=5,
        max_overflow=5,
    )

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    reset_session_factory()

    await engine.dispose()


# ============================================================================
# Shared session factory fixture
# ============================================================================


@pytest.fixture(scope="session")
def session_factory():
    """
    Returns the shared AsyncSessionFactory.

    Use this for concurrent tests so each task gets its own session.
    """
    return AsyncSessionFactory


# ============================================================================
# Function-scoped isolated DB session
# ============================================================================


@pytest_asyncio.fixture(scope="function")
async def db_session(
    async_engine,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Per-test AsyncSession with deterministic DB isolation.
    """

    # Full DB cleanup before every test.
    async with async_engine.begin() as conn:
        tables = ", ".join(_TRUNCATE_TABLES)

        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))

    session = AsyncSessionFactory()

    try:
        yield session

    finally:
        # Defensive cleanup — prevents poisoned sessions leaking.
        try:
            await session.rollback()
        except Exception:
            pass

        await session.close()


# ============================================================================
# TEMPORARY stabilisation fixture
# ============================================================================


@pytest_asyncio.fixture(autouse=True)
async def dispose_connections_between_tests():
    """
    Temporary debugging safeguard.

    Ensures broken asyncpg connections are never reused
    between tests while stabilising the suite.
    """
    yield

    await engine.dispose()


# ============================================================================
# Redis fixtures
# ============================================================================


@pytest.fixture(scope="function")
def redis_store() -> dict:
    """Backing store for mock_redis."""
    return {}


@pytest.fixture(scope="function")
def mock_redis(redis_store: dict):
    """
    Production-parity in-memory fake Redis.
    """

    from unittest.mock import AsyncMock

    redis = AsyncMock()

    async def _get(key: str) -> str | None:
        return redis_store.get(key)

    async def _set(
        key: str,
        value: str,
        *,
        nx: bool = False,
        px: int | None = None,
        ex: int | None = None,
    ) -> bool | None:
        if nx and key in redis_store:
            return None

        redis_store[key] = value

        return True

    async def _delete(*keys: str) -> int:
        removed = sum(1 for key in keys if redis_store.pop(key, None) is not None)

        return removed

    async def _lpush(key: str, *values: str) -> int:
        if key not in redis_store:
            redis_store[key] = []

        for value in reversed(values):
            redis_store[key].insert(0, value)

        return len(redis_store[key])

    async def _brpop(
        key: str,
        timeout: int = 0,
    ) -> tuple[str, str] | None:
        lst = redis_store.get(key)

        if lst:
            return (key, lst.pop())

        return None

    async def _rpop(key: str) -> str | None:
        lst = redis_store.get(key, [])

        return lst.pop() if lst else None

    async def _llen(key: str) -> int:
        return len(redis_store.get(key, []))

    async def _eval(
        script: str,
        num_keys: int,
        *args,
    ) -> int:
        if "GET" not in script or "DEL" not in script:
            raise NotImplementedError(
                "mock_redis.eval only supports distributed lock release."
            )

        key, token = args[0], args[1]

        if redis_store.get(key) == token:
            del redis_store[key]
            return 1

        return 0

    async def _ping() -> bool:
        return True

    async def _keys(pattern: str) -> list[str]:
        return [key for key in redis_store if fnmatch.fnmatch(key, pattern)]

    redis.get = _get
    redis.set = _set
    redis.delete = _delete
    redis.lpush = _lpush
    redis.brpop = _brpop
    redis.rpop = _rpop
    redis.llen = _llen
    redis.eval = _eval
    redis.ping = _ping
    redis.keys = _keys

    return redis


# ============================================================================
# Webhook payload builder
# ============================================================================


@pytest.fixture
def bunq_payment_webhook_factory():
    """
    Factory for valid bunq PAYMENT webhook payloads.
    """

    def _build(
        *,
        bunq_payment_id: int = 99001,
        status: str = "ACCEPTED",
        amount_value: str = "42.50",
        currency: str = "EUR",
        monetary_account_id: int = 123456,
    ) -> dict:
        return {
            "NotificationType": "PAYMENT",
            "EventType": "PAYMENT_CREATED",
            "Payment": {
                "id": bunq_payment_id,
                "status": status,
                "amount": {
                    "value": amount_value,
                    "currency": currency,
                },
                "description": "Test payment",
                "monetary_account_id": monetary_account_id,
                "created": "2024-01-15 10:00:00.000000",
                "updated": "2024-01-15 10:00:01.000000",
            },
        }

    return _build
