"""
worker_reconciliation.py — Standalone reconciliation worker process.

Run as a separate process from the API:
    python worker_reconciliation.py

Periodically reconciles internal ledger state against bunq's records and
recovers ambiguous payments (submitted to bunq but not yet confirmed).
"""

import asyncio
import logging
import signal

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.bunq.session_manager import get_session_manager
from app.infrastructure.db.session import close_db, init_db
from app.infrastructure.redis.client import close_redis, init_redis
from app.workers.reconciliation_worker import ReconciliationWorker

configure_logging()
logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    logger.info("Reconciliation worker starting")

    await init_db()
    await init_redis()

    session_manager = get_session_manager()
    await session_manager.bootstrap()

    worker = ReconciliationWorker(settings=settings)

    loop = asyncio.get_running_loop()

    def _handle_shutdown_signal():
        logger.info(
            "Reconciliation worker received shutdown signal — stopping after current tick"
        )
        worker.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_shutdown_signal)

    try:
        await worker.run()
    finally:
        logger.info("Reconciliation worker stopped — cleaning up")
        await close_redis()
        await close_db()
        logger.info("Reconciliation worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
