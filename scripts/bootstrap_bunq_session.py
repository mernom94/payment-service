"""
scripts/bootstrap_bunq_session.py — Standalone bunq sandbox session bootstrap.

Run this once before starting the application to establish and persist
the bunq sandbox session. Useful for CI pipelines and fresh deployments
where you want to verify connectivity before the full service starts.

Usage:
    python scripts/bootstrap_bunq_session.py

Environment variables required:
    BUNQ_API_KEY      — bunq sandbox API key
    DATABASE_URL      — PostgreSQL connection string
    REDIS_URL         — Redis connection string

Exit codes:
    0 — Session bootstrapped successfully.
    1 — Authentication failed (check BUNQ_API_KEY and network access).
"""

import asyncio
import logging
import sys

# Ensure the project root is on PYTHONPATH when running this script directly.
sys.path.insert(0, ".")

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.bunq.session_manager import BunqSessionManager
from app.infrastructure.db.session import close_db, init_db
from app.infrastructure.redis.client import close_redis, init_redis

configure_logging()
logger = logging.getLogger(__name__)


async def main() -> int:
    logger.info(
        "bootstrap.started", extra={"environment": get_settings().BUNQ_ENVIRONMENT}
    )

    if not get_settings().BUNQ_API_KEY:
        logger.error(
            "BUNQ_API_KEY is not set. "
            "Generate a sandbox API key at https://www.bunq.com/en/sandbox"
        )
        return 1

    try:
        await init_db()
        await init_redis()

        manager = BunqSessionManager()
        token = await manager.bootstrap()

        # Mask most of the token for log safety.
        masked = token[:6] + "..." + token[-4:] if len(token) > 10 else "***"
        logger.info(
            "bootstrap.success",
            extra={"session_token_preview": masked},
        )
        print(f"bunq sandbox session established. Token: {masked}")
        print("You can now start the application with: uvicorn main:app --reload\n")
        return 0

    except Exception as exc:
        logger.error("bootstrap.failed", extra={"error": str(exc)}, exc_info=True)
        print(f"Bootstrap failed: {exc}\n", file=sys.stderr)
        return 1

    finally:
        await close_redis()
        await close_db()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
