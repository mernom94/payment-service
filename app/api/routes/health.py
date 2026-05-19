"""
app/api/routes/health.py — Health check endpoints.

Two endpoints:
  GET /health        — Liveness probe. Returns 200 if the process is alive.
  GET /health/ready  — Readiness probe. Checks DB, Redis, and bunq session.
                       Returns 503 if any dependency is unhealthy so the load
                       balancer stops sending traffic until recovery.
"""

import logging
from typing import Any

from fastapi import APIRouter, Depends, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis
from app.infrastructure.bunq.session_manager import BunqSessionManager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def liveness() -> dict[str, str]:
    """Minimal liveness check — just confirms the process is up."""
    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(
    response: Response,
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
) -> dict[str, Any]:
    """
    Readiness check. Probes every critical dependency.

    Returns 200 if all are healthy, 503 with a per-dependency breakdown
    if any are not.
    """
    checks: dict[str, Any] = {}
    healthy = True

    # ── Database ──────────────────────────────────────────────────────────────
    try:
        await db.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        logger.error("Health check: database unreachable", exc_info=exc)
        checks["database"] = f"error: {exc}"
        healthy = False

    # ── Redis ─────────────────────────────────────────────────────────────────
    try:
        await redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        logger.error("Health check: Redis unreachable", exc_info=exc)
        checks["redis"] = f"error: {exc}"
        healthy = False

    # ── bunq session ──────────────────────────────────────────────────────────
    try:
        session_manager = BunqSessionManager()
        valid = await session_manager.is_session_valid()
        if valid:
            checks["bunq_session"] = "ok"
        else:
            checks["bunq_session"] = "invalid"
            healthy = False
    except Exception as exc:
        logger.error("Health check: bunq session check failed", exc_info=exc)
        checks["bunq_session"] = f"error: {exc}"
        healthy = False

    response.status_code = 200 if healthy else 503

    return {
        "status": "ready" if healthy else "degraded",
        "checks": checks,
    }
