"""
app/infrastructure/messaging/queue.py — Redis-backed async queues.

Simple LPUSH/BRPOP queue using Redis lists. This is appropriate for the
sandbox system. For production, replace with a proper message broker
(RabbitMQ, SQS, etc.) that supports dead-letter queues and visibility
timeouts.

Two queues:
  - WebhookQueue: receives webhook event IDs to be processed.
  - PaymentRetryQueue: receives payment IDs to be retried.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

WEBHOOK_QUEUE_KEY = "queue:webhooks"
PAYMENT_RETRY_QUEUE_KEY = "queue:payment_retries"


class BaseQueue:
    def __init__(self, redis, queue_key: str) -> None:
        self._redis = redis
        self._key = queue_key

    async def enqueue(self, item_id: str) -> None:
        """Push an item ID onto the left of the queue list."""
        await self._redis.lpush(self._key, item_id)
        logger.debug("queue.enqueue", extra={"queue": self._key, "item_id": item_id})

    async def dequeue(self, timeout: int = 5) -> Optional[str]:
        """
        Block until an item is available, then pop and return it.

        Returns None on timeout. The caller is responsible for processing
        the item and handling failures — there is no acknowledgement
        mechanism in this simple implementation. For production, use a
        two-phase pop with a processing set.
        """
        result = await self._redis.brpop(self._key, timeout=timeout)
        if result:
            _, item_id = result
            logger.debug(
                "queue.dequeue", extra={"queue": self._key, "item_id": item_id}
            )
            return item_id
        return None

    async def length(self) -> int:
        """Return the current queue depth."""
        return await self._redis.llen(self._key)

    async def drain(self) -> list[str]:
        """Remove and return all items (used in tests)."""
        items = []
        while True:
            item = await self._redis.rpop(self._key)
            if item is None:
                break
            items.append(item)
        return items


class WebhookQueue(BaseQueue):
    """Queue for pending webhook event IDs."""

    def __init__(self, redis) -> None:
        super().__init__(redis, WEBHOOK_QUEUE_KEY)


class PaymentRetryQueue(BaseQueue):
    """Queue for payment IDs that need to be retried."""

    def __init__(self, redis) -> None:
        super().__init__(redis, PAYMENT_RETRY_QUEUE_KEY)
