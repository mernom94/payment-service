"""
worker_webhook.py — Standalone webhook queue worker process.

Run as a separate process from the API:
    python worker_webhook.py

Processes webhook events from the Redis queue and calls WebhookProcessor.
Failed events are retried with exponential backoff up to MAX_WEBHOOK_RETRIES.
"""

import asyncio
import logging
import signal

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.db.session import close_db, init_db
from app.infrastructure.redis.client import close_redis, init_redis
from app.workers.webhook_worker import WebhookWorker

configure_logging()
logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    logger.info("Webhook worker starting")

    await init_db()
    await init_redis()

    worker = WebhookWorker(settings=settings)

    loop = asyncio.get_running_loop()

    def _handle_shutdown_signal():
        logger.info(
            "Webhook worker received shutdown signal — stopping after current tick"
        )
        worker.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    try:
        await worker.run()
    finally:
        logger.info("Webhook worker stopped — cleaning up")
        await close_redis()
        await close_db()
        logger.info("Webhook worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
