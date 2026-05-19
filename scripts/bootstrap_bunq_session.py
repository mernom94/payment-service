"""
Render-safe Bunq bootstrap script (idempotent + gated).

Behavior:
- If RUN_BUNQ_BOOTSTRAP != "true": exit safely (no-op)
- If Redis already has session: reuse it
- Otherwise: perform full Bunq bootstrap ONCE and persist session

This prevents Render redeploys from breaking Bunq device registration.
"""

import asyncio
import logging
import os
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
        extra={"env": get_settings().BUNQ_ENVIRONMENT},
    )

    # ---------------------------------------------------------
    # 0. Render-safe gate (CRITICAL for free tier)
    # ---------------------------------------------------------
    if os.getenv("RUN_BUNQ_BOOTSTRAP") != "true":
        logger.info("bootstrap.skipped.env_gate_not_set")
        print("Skipping Bunq bootstrap (RUN_BUNQ_BOOTSTRAP not set)")
        return 0

    if not get_settings().BUNQ_API_KEY:
        logger.error("BUNQ_API_KEY missing")
        return 1

    try:
        await init_db()
        await init_redis()

        redis = get_redis_client()

        # ---------------------------------------------------------
        # 1. Reuse existing session if available
        # ---------------------------------------------------------
        existing = await redis.get(SESSION_KEY)

        if existing:
            logger.info("bunq.session.reused")
            print("✔ Bunq session already exists in Redis")
            return 0

        # ---------------------------------------------------------
        # 2. Perform bootstrap only if missing
        # ---------------------------------------------------------
        logger.warning("bunq.session.missing_bootstrap_starting")

        manager = BunqSessionManager()
        token = await manager.bootstrap()

        if not token:
            raise RuntimeError("Bootstrap returned empty token")

        await redis.set(SESSION_KEY, token)

        masked = token[:6] + "..." + token[-4:] if len(token) > 10 else "***"

        logger.info(
            "bootstrap.success",
            extra={"session_token_preview": masked},
        )

        print(f"✔ Bunq session created: {masked}")
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