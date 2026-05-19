"""
scripts/reset_sandbox.py — Reset all local state for a clean sandbox run.

⚠️  DESTRUCTIVE. Clears:
  - All payments, ledger entries, webhook events, outbox records from the DB.
  - All bunq session records from the DB.
  - All Redis keys used by this application.

This does NOT reset the bunq sandbox itself (use the bunq sandbox portal or
API for that). It only resets the local orchestrator state.

Useful when:
  - Running integration tests that require a clean slate.
  - Recovering from a corrupted local state during development.
  - Switching to a different bunq sandbox API key.

Usage:
    python scripts/reset_sandbox.py [--confirm]

Pass --confirm to skip the interactive confirmation prompt.
"""

import asyncio
import logging
import sys

sys.path.insert(0, ".")

from app.core.logging import configure_logging
from app.infrastructure.db.session import close_db, get_engine, init_db
from app.infrastructure.redis.client import close_redis, get_redis_client, init_redis

configure_logging()
logger = logging.getLogger(__name__)

TABLES_TO_TRUNCATE = [
    "ledger_entries",  # Must come before ledger_accounts (FK).
    "ledger_accounts",
    "processed_webhook_events",
    "webhook_events",
    "outbox",
    "payments",
    "bunq_sessions",
]

REDIS_KEY_PATTERNS = [
    "idem:*",
    "lock:pay:*",
    "bunq:session",
    "bunq:reauth_lock",
    "queue:webhooks",
    "queue:payment_retries",
]


async def main() -> int:
    confirm = "--confirm" in sys.argv

    if not confirm:
        print("\n⚠️  This will DELETE ALL local orchestrator data.")
        print("Tables to truncate:", ", ".join(TABLES_TO_TRUNCATE))
        print("Redis keys to delete:", ", ".join(REDIS_KEY_PATTERNS))
        answer = input("\nType 'yes' to continue: ").strip().lower()
        if answer != "yes":
            print("Aborted.")
            return 0

    try:
        await init_db()
        await init_redis()

        engine = await get_engine()
        redis = await get_redis_client()

        # ── Truncate DB tables ────────────────────────────────────────────────
        async with engine.begin() as conn:
            # Disable triggers temporarily (e.g. the ledger immutability triggers).
            await conn.execute(
                __import__("sqlalchemy").text("SET session_replication_role = replica")
            )
            for table in TABLES_TO_TRUNCATE:
                await conn.execute(
                    __import__("sqlalchemy").text(f"TRUNCATE TABLE {table} CASCADE")
                )
                logger.info(f"Truncated table: {table}")

            await conn.execute(
                __import__("sqlalchemy").text("SET session_replication_role = DEFAULT")
            )

        print(f"✅  Truncated {len(TABLES_TO_TRUNCATE)} tables.")

        # ── Flush Redis keys ──────────────────────────────────────────────────
        deleted = 0
        for pattern in REDIS_KEY_PATTERNS:
            keys = await redis.keys(pattern)
            if keys:
                await redis.delete(*keys)
                deleted += len(keys)

        print(f"✅  Deleted {deleted} Redis keys.")
        logger.info("reset_sandbox.completed", extra={"deleted_redis_keys": deleted})
        return 0

    except Exception as exc:
        logger.error("reset_sandbox.failed", extra={"error": str(exc)}, exc_info=True)
        print(f"\n❌  Reset failed: {exc}\n", file=sys.stderr)
        return 1

    finally:
        await close_redis()
        await close_db()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
