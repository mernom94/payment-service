"""
app/infrastructure/redis/idempotency.py — Idempotency result cache.

Caches payment_id by external_id so repeated identical requests return
immediately without hitting the database.

TTL is set to 24 hours — long enough to catch most retries, short enough
not to grow forever.
"""

import logging
from typing import Optional

from app.core.config import get_settings

logger = logging.getLogger(__name__)


class IdempotencyStore:
    def __init__(self, redis) -> None:
        self._redis = redis
        self._ttl = get_settings().REDIS_IDEMPOTENCY_TTL_SECONDS

    async def get(self, key: str) -> Optional[str]:
        """
        Return the cached value for key, or None if not present.
        """
        try:
            value = await self._redis.get(key)
            return value
        except Exception as exc:
            # Redis miss — fall through to DB lookup.
            # We log but don't raise: the system can function without the cache.
            logger.warning(
                "idempotency.cache.get_failed",
                extra={"key": key, "error": str(exc)},
            )
            return None

    async def set(self, key: str, value: str) -> None:
        """
        Cache value for key with the configured TTL.
        """
        try:
            await self._redis.set(key, value, ex=self._ttl)
        except Exception as exc:
            # Cache write failure is non-fatal — the DB is authoritative.
            logger.warning(
                "idempotency.cache.set_failed",
                extra={"key": key, "error": str(exc)},
            )

    async def delete(self, key: str) -> None:
        """Remove a cached entry (e.g. to force re-lookup after a state change)."""
        try:
            await self._redis.delete(key)
        except Exception as exc:
            logger.warning(
                "idempotency.cache.delete_failed",
                extra={"key": key, "error": str(exc)},
            )
