"""
app/api/deps.py — FastAPI dependency injection and exception handlers.

Provides reusable dependencies (DB session, Redis client, service instances)
and translates domain exceptions into appropriate HTTP responses. Keeping
HTTP status code decisions here means domain code never imports FastAPI.
"""

import logging
from typing import AsyncGenerator

from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import (
    BunqAPIError,
    BunqNetworkError,
    BunqSessionError,
    DuplicatePaymentError,
    DuplicateWebhookError,
    IdempotencyLockError,
    InvalidPaymentStateError,
    LedgerImbalanceError,
    OrchestratorError,
    PaymentNotFoundError,
    PaymentValidationError,
    ReconciliationError,
)
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.redis.client import get_redis_client

logger = logging.getLogger(__name__)


# ── Database session dependency ───────────────────────────────────────────────


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yields a SQLAlchemy async session via the injected factory.

    Committed on success, rolled back on any exception. The factory is
    resolved at call time (not import time) so tests can patch
    get_session_factory() before the first request.
    """
    async with get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ── Redis dependency ──────────────────────────────────────────────────────────


async def get_redis():
    """Yields the shared Redis client."""
    return await get_redis_client()


# ── Correlation ID dependency ─────────────────────────────────────────────────


def get_correlation_id(request: Request) -> str:
    """
    Extracts the correlation_id injected by CorrelationIDMiddleware.
    Always present — the middleware guarantees it.
    """
    return request.state.correlation_id


# ── Exception → HTTP response mapping ────────────────────────────────────────


def register_exception_handlers(app) -> None:
    """
    Register all domain exception → HTTP response mappings.
    Call once during app construction (in main.py or the app factory).
    """

    @app.exception_handler(PaymentNotFoundError)
    async def payment_not_found_handler(request: Request, exc: PaymentNotFoundError):
        return JSONResponse(status_code=404, content={"error": exc.message})

    @app.exception_handler(DuplicatePaymentError)
    async def duplicate_payment_handler(request: Request, exc: DuplicatePaymentError):
        # 200 (not 409) — idempotent: return the existing payment, not an error body.
        # The existing payment object is attached to the exception when available
        # (service layer fetches it before raising); fall back to a DB lookup only
        # if it wasn't carried along (e.g. raised from the outbox flush path).
        from app.domain.payments.models import PaymentResponse

        if exc.existing_payment is not None:
            return JSONResponse(
                status_code=200,
                content=PaymentResponse.model_validate(exc.existing_payment).model_dump(
                    mode="json"
                ),
            )
        # Fallback: the payment object wasn't carried; return a minimal indicator.
        # In practice this path should never be reached with the current service code.
        logger.warning(
            "duplicate_payment_handler.no_payment_object",
            extra={"external_id": exc.external_id},
        )
        return JSONResponse(
            status_code=200,
            content={"error": "duplicate_request", "detail": exc.message},
        )

    @app.exception_handler(PaymentValidationError)
    async def payment_validation_handler(request: Request, exc: PaymentValidationError):
        return JSONResponse(status_code=422, content={"error": exc.message})

    @app.exception_handler(InvalidPaymentStateError)
    async def invalid_state_handler(request: Request, exc: InvalidPaymentStateError):
        return JSONResponse(status_code=409, content={"error": exc.message})

    @app.exception_handler(LedgerImbalanceError)
    async def ledger_imbalance_handler(request: Request, exc: LedgerImbalanceError):
        # This is a critical system invariant violation.
        logger.critical(
            "LEDGER IMBALANCE — this is a critical error requiring immediate investigation",
            exc_info=exc,
            extra={"imbalance": exc.imbalance},
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "detail": "Ledger integrity violation.",
            },
        )

    @app.exception_handler(DuplicateWebhookError)
    async def duplicate_webhook_handler(request: Request, exc: DuplicateWebhookError):
        # Return 200 so bunq does not retry — we've already processed this.
        return JSONResponse(status_code=200, content={"status": "already_processed"})

    @app.exception_handler(IdempotencyLockError)
    async def idempotency_lock_handler(request: Request, exc: IdempotencyLockError):
        # Include Retry-After so well-behaved callers back off correctly
        # rather than immediately hammering the endpoint again.
        return JSONResponse(
            status_code=409,
            content={
                "error": "conflict",
                "detail": "Request in progress. Retry shortly.",
            },
            headers={"Retry-After": "2"},
        )

    @app.exception_handler(BunqSessionError)
    async def bunq_session_handler(request: Request, exc: BunqSessionError):
        logger.error("bunq session error", exc_info=exc)
        return JSONResponse(
            status_code=503,
            content={
                "error": "service_unavailable",
                "detail": "Payment provider session invalid.",
            },
        )

    @app.exception_handler(BunqAPIError)
    async def bunq_api_error_handler(request: Request, exc: BunqAPIError):
        logger.error(
            "bunq API error", status_code=exc.status_code, detail=exc.bunq_message
        )
        if exc.status_code == 429:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limited",
                    "detail": "Payment provider rate limit hit.",
                },
            )
        return JSONResponse(
            status_code=502,
            content={"error": "bad_gateway", "detail": exc.bunq_message},
        )

    @app.exception_handler(BunqNetworkError)
    async def bunq_network_handler(request: Request, exc: BunqNetworkError):
        logger.warning("bunq network error", exc_info=exc)
        return JSONResponse(
            status_code=503,
            content={
                "error": "service_unavailable",
                "detail": "Payment provider unreachable.",
            },
        )

    @app.exception_handler(ReconciliationError)
    async def reconciliation_handler(request: Request, exc: ReconciliationError):
        logger.error(
            "Reconciliation drift",
            account_id=exc.account_id,
            internal=exc.internal,
            external=exc.external,
        )
        return JSONResponse(
            status_code=500,
            content={"error": "reconciliation_drift", "detail": exc.message},
        )

    @app.exception_handler(OrchestratorError)
    async def generic_orchestrator_handler(request: Request, exc: OrchestratorError):
        logger.error("Unhandled orchestrator error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={"error": "internal_error", "detail": exc.message},
        )

    @app.exception_handler(Exception)
    async def generic_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "detail": "An unexpected error occurred.",
            },
        )
