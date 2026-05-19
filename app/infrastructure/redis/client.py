"""
app/infrastructure/redis/client.py — Redis async client.

Provides a single shared Redis connection pool initialised at startup.
Workers and API routes access it via get_redis_client().
"""

import logging

import redis.asyncio as aioredis

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_redis_client: aioredis.Redis | None = None


async def init_redis() -> None:
    """
    Initialise the Redis connection pool.
    Call once at application startup.
    """
    global _redis_client
    _redis_client = aioredis.from_url(
        str(get_settings().REDIS_URL),
        encoding="utf-8",
        decode_responses=True,
        max_connections=20,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    # Verify connectivity.
    await _redis_client.ping()
    logger.info("Redis connected", extra={"url": str(get_settings().REDIS_URL)})


async def get_redis_client() -> aioredis.Redis:
    if _redis_client is None:
        raise RuntimeError("Redis client not initialised. Call init_redis() first.")
    return _redis_client


async def close_redis() -> None:
    """Close the Redis connection pool at shutdown."""
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection closed")
