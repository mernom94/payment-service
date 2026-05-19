"""
app/domain/payments/service.py — Payment orchestration service.

This is the heart of the payment flow. It coordinates:
  1. Idempotency enforcement (Redis lock + DB unique constraint)
  2. Input validation
  3. Atomic write of payment + outbox record
  4. Returning the result to the API layer

The actual bunq API call is NOT made here — it happens in the payment worker
which picks up the outbox record. This design means the API response is fast
and the caller gets a consistent result even if bunq is slow or down.
"""

import logging
import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import (
    REDIS_IDEMPOTENCY_PREFIX,
    REDIS_PAYMENT_LOCK_PREFIX,
    OutboxStatus,
    PaymentState,
)
from app.core.exceptions import (
    DuplicatePaymentError,
    IdempotencyLockError,
    PaymentNotFoundError,
)
from app.domain.payments.models import (
    CreatePaymentRequest,
    Payment,
    PaymentListResponse,
    PaymentResponse,
)
from app.domain.payments.validators import validate_create_payment_request
from app.infrastructure.db.outbox import Outbox
from app.infrastructure.redis.idempotency import IdempotencyStore
from app.infrastructure.redis.locks import DistributedLock

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, db: AsyncSession, redis) -> None:
        self._db = db
        self._redis = redis
        self._idempotency = IdempotencyStore(redis)
        self._lock = DistributedLock(redis)

    # ── Create ────────────────────────────────────────────────────────────────

    async def create_payment(
        self,
        request: CreatePaymentRequest,
        *,
        correlation_id: Optional[str] = None,
    ) -> PaymentResponse:
        """
        Create a new payment or return an existing one for the same external_id.

        Idempotency strategy (three layers):
          Layer 1 — Redis idempotency cache: if we've already returned a result
                    for this external_id, return the cached payment_id immediately
                    (fastest path, no DB needed).
          Layer 2 — Redis distributed lock: prevents two concurrent requests
                    with the same external_id from both proceeding past the
                    duplicate check at the same time.
          Layer 3 — DB UNIQUE constraint on external_id: last line of defence
                    if Redis is down or the lock expires unexpectedly. The
                    IntegrityError is caught and converted to DuplicatePaymentError.

        After all checks pass, writes the Payment + Outbox record in a single
        DB transaction. The worker picks up the outbox record and calls bunq.
        """
        external_id = request.external_id

        # Layer 1: Fast path — cached result from a previous successful request.
        cached_id = await self._idempotency.get(
            f"{REDIS_IDEMPOTENCY_PREFIX}{external_id}"
        )
        if cached_id:
            logger.info(
                "payment.idempotency.cache_hit",
                extra={"external_id": external_id, "cached_payment_id": cached_id},
            )
            existing = await self._get_payment_by_id(uuid.UUID(cached_id))
            if existing:
                return PaymentResponse.model_validate(existing)

        # Layer 2: Acquire distributed lock before querying DB.
        lock_key = f"{REDIS_PAYMENT_LOCK_PREFIX}{external_id}"
        async with self._lock.acquire(lock_key, timeout=30) as acquired:
            if not acquired:
                raise IdempotencyLockError(
                    f"Could not acquire idempotency lock for external_id={external_id!r}. "
                    "A concurrent request is already processing this payment."
                )

            # Check DB under the lock — another request may have committed
            # between the cache miss and lock acquisition.
            existing_payment = await self._get_payment_by_external_id(external_id)
            if existing_payment:
                logger.info(
                    "payment.idempotency.db_hit",
                    extra={
                        "external_id": external_id,
                        "payment_id": str(existing_payment.id),
                    },
                )
                # Backfill the cache so future requests hit Layer 1.
                await self._idempotency.set(
                    f"{REDIS_IDEMPOTENCY_PREFIX}{external_id}",
                    str(existing_payment.id),
                )
                # Skip validation: the payment was already validated when first
                # created. Re-running IBAN MOD-97 and other checks on a known-good
                # record that already exists in the DB is wasteful and misleading.
                return PaymentResponse.model_validate(existing_payment)

            # Validate inputs — only reached for genuinely new payments.
            # Cache hits and DB hits return above; this path only runs once per
            # unique external_id across the lifetime of the system.
            validated_iban, validated_currency, validated_amount, _ = (
                validate_create_payment_request(
                    external_id=external_id,
                    from_account_id=request.from_account_id,
                    to_iban=request.to_iban,
                    amount=request.amount,
                    currency=request.currency,
                )
            )

            # Create payment + outbox in a single atomic transaction.
            payment = await self._create_payment_and_outbox(
                external_id=external_id,
                from_account_id=request.from_account_id,
                to_iban=validated_iban,
                amount=validated_amount,
                currency=validated_currency,
                description=request.description,
                correlation_id=correlation_id,
            )

            # Populate idempotency cache so subsequent duplicates are fast.
            await self._idempotency.set(
                f"{REDIS_IDEMPOTENCY_PREFIX}{external_id}",
                str(payment.id),
            )

            logger.info(
                "payment.created",
                extra={
                    "payment_id": str(payment.id),
                    "external_id": external_id,
                    "amount": str(validated_amount),
                    "currency": validated_currency,
                },
            )

            return PaymentResponse.model_validate(payment)

    async def _create_payment_and_outbox(
        self,
        *,
        external_id: str,
        from_account_id: str,
        to_iban: str,
        amount: Decimal,
        currency: str,
        description: Optional[str],
        correlation_id: Optional[str],
    ) -> Payment:
        """
        Write Payment and Outbox records atomically.

        Using a single transaction guarantees that if the DB commit succeeds,
        the worker will always find an outbox record to process. If the commit
        fails, neither record exists and the caller can safely retry.
        """
        payment = Payment(
            id=uuid.uuid4(),
            external_id=external_id,
            from_account_id=from_account_id,
            to_iban=to_iban,
            amount=amount,
            currency=currency,
            description=description,
            state=PaymentState.PENDING,
            correlation_id=correlation_id,
        )
        self._db.add(payment)

        outbox = Outbox(
            id=uuid.uuid4(),
            payment_id=payment.id,
            status=OutboxStatus.PENDING,
            payload={
                "payment_id": str(payment.id),
                "external_id": external_id,
                "from_account_id": from_account_id,
                "to_iban": to_iban,
                "amount": str(amount),
                "currency": currency,
                "description": description,
            },
        )
        self._db.add(outbox)

        try:
            await self._db.flush()  # Get DB-generated values (e.g. created_at).
        except IntegrityError:
            # Layer 3: DB unique constraint caught a race we didn't catch above.
            await self._db.rollback()
            existing = await self._get_payment_by_external_id(external_id)
            if existing:
                raise DuplicatePaymentError(external_id, existing_payment=existing)
            raise  # Unexpected integrity error — re-raise.

        return payment

    # ── Read ──────────────────────────────────────────────────────────────────

    async def get_payment(self, payment_id: uuid.UUID) -> PaymentResponse:
        payment = await self._get_payment_by_id(payment_id)
        if not payment:
            raise PaymentNotFoundError(f"Payment {payment_id} not found.")
        return PaymentResponse.model_validate(payment)

    async def list_payments(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        state: Optional[str] = None,
    ) -> PaymentListResponse:
        """
        Return a paginated list of payments.

        Uses COUNT(*) OVER () as a window function so the total count and the
        page data come from the same query snapshot, eliminating the race where
        a separate count query could reflect a different state than the data
        query under concurrent inserts (major concern from review).
        """
        from sqlalchemy import func as sqlfunc

        base_query = select(Payment)
        if state:
            base_query = base_query.where(Payment.state == state)

        # Window function: count all matching rows in the same query.
        windowed = base_query.add_columns(
            sqlfunc.count().over().label("total_count")
        ).order_by(Payment.created_at.desc())

        offset = (page - 1) * page_size
        windowed = windowed.offset(offset).limit(page_size)

        result = await self._db.execute(windowed)
        rows = result.all()

        if rows:
            payments = [row[0] for row in rows]
            total = rows[0][1]
        else:
            payments = []
            # No rows on this page — run a dedicated count for the total.
            count_query = select(func.count()).select_from(Payment)
            if state:
                count_query = count_query.where(Payment.state == state)
            count_result = await self._db.execute(count_query)
            total = count_result.scalar_one()

        return PaymentListResponse(
            items=[PaymentResponse.model_validate(p) for p in payments],
            total=total,
            page=page,
            page_size=page_size,
        )

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _get_payment_by_id(self, payment_id: uuid.UUID) -> Optional[Payment]:
        result = await self._db.execute(select(Payment).where(Payment.id == payment_id))
        return result.scalar_one_or_none()

    async def _get_payment_by_external_id(self, external_id: str) -> Optional[Payment]:
        result = await self._db.execute(
            select(Payment).where(Payment.external_id == external_id)
        )
        return result.scalar_one_or_none()
