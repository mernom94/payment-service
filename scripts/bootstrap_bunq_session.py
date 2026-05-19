"""
scripts/bootstrap_bunq_session.py — Bunq sandbox session bootstrap (idempotent).

This script is SAFE for production deployments (e.g. Render) because it:
- Reuses existing session from Redis if available
- Only performs full bootstrap if no session exists

Recommended usage:
    - Run manually once (local or admin action)
    - NOT as part of every deploy startup
"""

import asyncio
import logging
import sys

sys.path.insert(0, ".")

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.infrastructure.bunq.session_manager import BunqSessionManager
from app.infrastructure.db.session import close_db, init_db
from app.infrastructure.redis.client import close_redis, init_redis, get_redis_client

configure_logging()
logger = logging.getLogger(__name__)


SESSION_KEY = "bunq:session_token"


async def main() -> int:
    logger.info(
        "bootstrap.started",
        extra={"environment": get_settings().BUNQ_ENVIRONMENT},
    )

    if not get_settings().BUNQ_API_KEY:
        logger.error("BUNQ_API_KEY is not set")
        return 1

    try:
        await init_db()
        await init_redis()

        redis = get_redis_client()

        # ---------------------------------------------------------
        # 1. Try reuse existing session (CRITICAL FOR RENDER)
        # ---------------------------------------------------------
        existing_token = await redis.get(SESSION_KEY)

        if existing_token:
            logger.info("bunq.session.reused")
            print("✔ Using existing Bunq session (no bootstrap needed)")
            return 0

        # ---------------------------------------------------------
        # 2. Only bootstrap if missing
        # ---------------------------------------------------------
        logger.warning("bunq.session.missing_bootstrap_required")

        manager = BunqSessionManager()
        token = await manager.bootstrap()

        if not token:
            raise RuntimeError("Bootstrap returned empty session token")

        await redis.set(SESSION_KEY, token)

        masked = token[:6] + "..." + token[-4:] if len(token) > 10 else "***"

        logger.info(
            "bootstrap.success",
            extra={"session_token_preview": masked},
        )

        print(f"✔ Bunq session established: {masked}")
        return 0

    except Exception as exc:
        logger.error("bootstrap.failed", extra={"error": str(exc)}, exc_info=True)
        print(f"Bootstrap failed: {exc}", file=sys.stderr)
        return 1

    finally:
        await close_redis()
        await close_db()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))