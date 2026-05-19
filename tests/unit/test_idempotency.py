"""
tests/unit/test_idempotency.py — Unit tests for idempotency enforcement.

Tests the Redis idempotency store and distributed lock independently.
Uses the mock_redis fixture — no real Redis required.
"""

from unittest.mock import AsyncMock

import pytest

from app.infrastructure.redis.idempotency import IdempotencyStore
from app.infrastructure.redis.locks import DistributedLock


class TestIdempotencyStore:
    @pytest.mark.asyncio
    async def test_get_returns_none_on_miss(self, mock_redis):
        store = IdempotencyStore(mock_redis)
        result = await store.get("idem:nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_then_get_returns_value(self, mock_redis):
        store = IdempotencyStore(mock_redis)
        await store.set("idem:test-001", "payment-uuid-123")
        result = await store.get("idem:test-001")
        assert result == "payment-uuid-123"

    @pytest.mark.asyncio
    async def test_delete_clears_key(self, mock_redis):
        store = IdempotencyStore(mock_redis)
        await store.set("idem:test-002", "some-id")
        await store.delete("idem:test-002")
        result = await store.get("idem:test-002")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_does_not_raise_on_redis_error(self, mock_redis):
        """Cache miss should not propagate Redis errors."""
        mock_redis.get = AsyncMock(side_effect=Exception("Redis unavailable"))
        store = IdempotencyStore(mock_redis)
        result = await store.get("idem:any")
        assert result is None  # Graceful degradation.

    @pytest.mark.asyncio
    async def test_set_does_not_raise_on_redis_error(self, mock_redis):
        """Cache write failure should not propagate."""
        mock_redis.set = AsyncMock(side_effect=Exception("Redis unavailable"))
        store = IdempotencyStore(mock_redis)
        await store.set("idem:any", "value")  # Must not raise.


class TestDistributedLock:
    @pytest.mark.asyncio
    async def test_acquire_yields_true_when_available(self, mock_redis):
        lock = DistributedLock(mock_redis)
        async with lock.acquire("lock:test-key") as acquired:
            assert acquired is True

    @pytest.mark.asyncio
    async def test_acquire_yields_false_when_already_held(self, mock_redis):
        lock = DistributedLock(mock_redis)
        # Acquire the lock and hold it while we try to acquire again.
        async with lock.acquire("lock:contested") as first:
            assert first is True
            # Try to acquire the same key with no retries.
            inner_lock = DistributedLock(mock_redis)
            async with inner_lock.acquire("lock:contested", retry_count=0) as second:
                assert second is False

    @pytest.mark.asyncio
    async def test_lock_released_after_context_exit(self, mock_redis):
        lock = DistributedLock(mock_redis)
        async with lock.acquire("lock:sequential"):
            pass  # Lock released here.

        # Should be able to acquire again immediately.
        async with lock.acquire("lock:sequential") as acquired:
            assert acquired is True

    @pytest.mark.asyncio
    async def test_lock_released_on_exception(self, mock_redis):
        lock = DistributedLock(mock_redis)
        try:
            async with lock.acquire("lock:exception-test") as acquired:
                assert acquired is True
                raise ValueError("Simulated error")
        except ValueError:
            pass

        # Lock should be released despite the exception.
        async with lock.acquire("lock:exception-test") as acquired:
            assert acquired is True

    @pytest.mark.asyncio
    async def test_lua_release_is_atomic(self, mock_redis):
        """
        Verify the Lua script only releases if the token matches.
        Simulates the case where the lock expired and was acquired by someone else.
        """
        lock = DistributedLock(mock_redis)
        # Manually set a different token in Redis before releasing.
        await mock_redis.set("lock:stolen", "other-owner-token", nx=False)
        # The release (with our token) should be a no-op.
        await lock._release("lock:stolen", "our-token")
        # The key should still exist with the other owner's token.
        assert await mock_redis.get("lock:stolen") == "other-owner-token"
