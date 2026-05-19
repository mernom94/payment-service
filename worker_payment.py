"""
worker_payment.py — Standalone payment outbox worker process.

Run as a separate process from the API:
    python worker_payment.py

Graceful shutdown:
  - SIGTERM / SIGINT signals the worker to stop polling after the current
    tick completes.  In-flight DB transactions are committed before exit.
  - The worker does NOT call task.cancel() mid-tick; it sets a stop flag
    that BaseWorker checks between ticks.  This means a payment that is
    mid-bunq-call will complete and commit before the process exits.
"""

import asyncio
import logging
import signal

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.bunq.session_manager import get_session_manager
from app.infrastructure.db.session import close_db, init_db
from app.infrastructure.redis.client import close_redis, init_redis
from app.workers.payment_worker import PaymentWorker

configure_logging()
logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    logger.info("Payment worker starting")

    await init_db()
    await init_redis()

    session_manager = get_session_manager()
    await session_manager.bootstrap()

    worker = PaymentWorker(settings=settings)

    # Graceful shutdown: on SIGTERM/SIGINT, set the stop flag on BaseWorker
    # so the worker finishes its current tick and exits cleanly.
    loop = asyncio.get_running_loop()

    def _handle_shutdown_signal():
        logger.info(
            "Payment worker received shutdown signal — stopping after current tick"
        )
        worker.stop()  # Sets BaseWorker._stop = True; loop exits after tick completes.

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    try:
        await worker.run()
    finally:
        logger.info("Payment worker stopped — cleaning up")
        await close_redis()
        await close_db()
        logger.info("Payment worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
