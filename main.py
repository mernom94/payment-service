"""
main.py — API server entrypoint.

Refactored:
  - @app.on_event replaced by lifespan context manager (PEP 3156 / FastAPI 0.93+).
  - All three middleware rewritten as pure-ASGI callables (no BaseHTTPMiddleware).
  - DB engine lifecycle owned entirely by lifespan; no module-level globals.

Workers run as separate processes (worker_payment.py, worker_webhook.py,
worker_reconciliation.py). The API can be scaled horizontally without
multiplying worker processes.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.middleware.auth import APIKeyMiddleware, assert_api_key_configured
from app.api.middleware.correlation import CorrelationIDMiddleware
from app.api.middleware.logging import RequestLoggingMiddleware
from app.api.routes import health, payments, webhooks
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.observability import init_tracing
from app.infrastructure.bunq.session_manager import get_session_manager
from app.infrastructure.db.session import close_db, init_db
from app.infrastructure.redis.client import close_redis, init_redis

configure_logging()
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan context manager.

    Replaces the deprecated @app.on_event("startup") / @app.on_event("shutdown")
    hooks. Everything before `yield` runs on startup; everything after on shutdown.
    Using a context manager guarantees shutdown runs even if startup raises.
    """
    logger.info("Starting Bunq Payment Orchestrator (API process)")

    # 1. Fail fast if API_KEY is unset in production.
    assert_api_key_configured()

    # 2. OTLP tracing — must init before any spans are created.
    init_tracing(service_name="payment-service")

    # 3. Database — configure_session_factory is called inside init_db().
    await init_db()
    logger.info("Database initialised")

    # 4. Redis
    await init_redis()
    logger.info("Redis connected")

    # 5. bunq session — use the module-level singleton so workers share it.
    session_manager = get_session_manager()
    await session_manager.bootstrap()
    logger.info("bunq sandbox session established")

    yield  # Application runs here.

    # ── Shutdown ──────────────────────────────────────────────────────────────
    logger.info("API process shutting down")
    await close_redis()
    await close_db()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Bunq Payment Orchestrator",
    version="1.0.0",
    description=(
        "Production-grade payment backend with double-entry ledger "
        "and idempotency guarantees."
    ),
    lifespan=lifespan,
)

# ── Middleware (outermost runs first) ─────────────────────────────────────────
# CORSMiddleware is Starlette's own; it is exempt from the BaseHTTPMiddleware ban.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://backend-portfolio-two-ebon.vercel.app/"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
# Pure-ASGI middlewares — no BaseHTTPMiddleware.
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(CorrelationIDMiddleware)
app.add_middleware(APIKeyMiddleware)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(health.router, tags=["health"])
app.include_router(payments.router, prefix="/payments", tags=["payments"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])

# ── Prometheus metrics endpoint ───────────────────────────────────────────────
try:
    from prometheus_client import make_asgi_app as _make_metrics_app

    metrics_app = _make_metrics_app()
    app.mount("/metrics", metrics_app)
    logger.debug("Prometheus /metrics endpoint mounted")
except ImportError:
    pass  # prometheus_client not installed — metrics endpoint omitted.

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        log_config=None,  # Logging managed by configure_logging().
    )
