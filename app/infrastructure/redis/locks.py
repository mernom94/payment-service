"""
app/infrastructure/redis/locks.py — Distributed lock using Redis SET NX.

Used to prevent concurrent processing of the same payment. The lock is
released either explicitly (on success or failure) or automatically via TTL
(on worker crash).

Uses Redis SET NX PX (set-if-not-exists with millisecond TTL) — not
Redlock — which is correct for a single Redis instance. For a multi-node
Redis cluster, Redlock would be needed.
"""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

logger = logging.getLogger(__name__)


class DistributedLock:
    def __init__(self, redis) -> None:
        self._redis = redis

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: int = 30,
        retry_count: int = 3,
        retry_delay: float = 0.1,
    ) -> AsyncIterator[bool]:
        """
        Async context manager that acquires a Redis lock.

        Yields True if the lock was acquired, False if it wasn't (after retries).
        The lock is always released on context exit.

        Uses a unique token per lock acquisition to prevent a process from
        accidentally releasing a lock it doesn't own (e.g. after a long
        operation causes the TTL to expire and another process acquires the lock).

        Usage:
            async with lock.acquire("lock:key", timeout=30) as acquired:
                if not acquired:
                    raise IdempotencyLockError(...)
                # ... do work ...
        """
        lock_token = str(uuid.uuid4())
        lock_ttl_ms = timeout * 1000
        acquired = False

        for attempt in range(retry_count + 1):
            result = await self._redis.set(key, lock_token, nx=True, px=lock_ttl_ms)
            if result:
                acquired = True
                break
            if attempt < retry_count:
                await asyncio.sleep(retry_delay * (2**attempt))  # Exponential backoff.

        try:
            yield acquired
        finally:
            if acquired:
                await self._release(key, lock_token)

    async def _release(self, key: str, token: str) -> None:
        """
        Release the lock only if we still own it (token matches).

        Uses a Lua script for atomicity — the check and delete must be
        a single operation to prevent a race between checking ownership
        and deleting the key.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        result = await self._redis.eval(lua_script, 1, key, token)
        if result == 0:
            logger.warning(
                "lock.release.not_owner — lock expired or taken by another process",
                extra={"key": key},
            )
