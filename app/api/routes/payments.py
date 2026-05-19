"""
app/api/routes/payments.py — Payment API endpoints.

POST /payments         — Create (or idempotently retrieve) a payment.
GET  /payments/{id}    — Fetch a payment by internal ID.
GET  /payments         — List payments (paginated).

The route handlers are intentionally thin: validate input, call the service,
return the result. All business logic lives in the domain layer.
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis, get_correlation_id
from app.domain.payments.service import PaymentService
from app.domain.payments.models import (
    CreatePaymentRequest,
    PaymentResponse,
    PaymentListResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_payment_service(
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
) -> PaymentService:
    return PaymentService(db=db, redis=redis)


@router.post("", response_model=PaymentResponse, status_code=201)
async def create_payment(
    request: CreatePaymentRequest,
    correlation_id: str = Depends(get_correlation_id),
    service: PaymentService = Depends(_get_payment_service),
) -> PaymentResponse:
    """
    Create a payment.

    Idempotent: if a payment with the same `external_id` already exists,
    the existing payment is returned rather than creating a duplicate.
    The HTTP status will be 200 (not 201) in that case — the exception
    handler in deps.py handles this transparently.

    The payment is written to the database and outbox atomically. The actual
    bunq API call happens asynchronously via the payment worker.
    """
    logger.info(
        "payments.create.requested",
        extra={
            "external_id": request.external_id,
            "amount": str(request.amount),
            "currency": request.currency,
            "correlation_id": correlation_id,
        },
    )

    payment = await service.create_payment(request, correlation_id=correlation_id)

    logger.info(
        "payments.create.accepted",
        extra={
            "payment_id": str(payment.id),
            "external_id": payment.external_id,
            "state": payment.state,
        },
    )
    return payment


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(
    payment_id: UUID,
    service: PaymentService = Depends(_get_payment_service),
) -> PaymentResponse:
    """Fetch a single payment by its internal UUID."""
    return await service.get_payment(payment_id)


@router.get("", response_model=PaymentListResponse)
async def list_payments(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    state: Optional[str] = Query(default=None),
    service: PaymentService = Depends(_get_payment_service),
) -> PaymentListResponse:
    """
    List payments with optional state filter and pagination.

    Returns payments in reverse-chronological order (newest first).
    """
    # Validate state against the enum before passing to the service layer.
    # Without this check, arbitrary strings reach the SQL WHERE clause which,
    # while parameterised (safe from injection), silently returns empty results
    # for any typo and gives no feedback to callers.
    from app.core.constants import PaymentState

    if state is not None:
        try:
            PaymentState(state.upper())
            state = state.upper()
        except ValueError:
            from fastapi import HTTPException

            valid = [s.value for s in PaymentState]
            raise HTTPException(
                status_code=422,
                detail=f"Invalid state filter {state!r}. Must be one of: {valid}",
            )
    return await service.list_payments(page=page, page_size=page_size, state=state)
