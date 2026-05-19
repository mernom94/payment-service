"""
tests/integration/payment/test_payment_service.py — Integration tests for PaymentService.

Tests the full payment creation flow including idempotency, duplicate detection,
state persistence, and outbox record creation.

Architecture:
  - GIVEN  : test data is set up exclusively via PaymentFactory / OutboxFactory.
  - WHEN   : only public PaymentService methods are called.
  - THEN   : assertions are made against service return values or DB queries.

No ORM models are instantiated directly in this file.
"""

import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.constants import OutboxStatus, PaymentState
from app.core.exceptions import (
    DuplicatePaymentError,
    IdempotencyLockError,
    PaymentNotFoundError,
)
from app.domain.payments.models import CreatePaymentRequest
from app.domain.payments.service import PaymentService
from app.infrastructure.db.outbox import Outbox
from tests.factories import PaymentFactory


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_request(
    *,
    external_id: str | None = None,
    amount: Decimal = Decimal("42.50"),
    currency: str = "EUR",
    to_iban: str = "NL02ABNA0123456789",
    from_account_id: str = "123456",
    description: str | None = "Test payment",
) -> CreatePaymentRequest:
    return CreatePaymentRequest(
        external_id=external_id or f"ext-{uuid.uuid4().hex[:12]}",
        from_account_id=from_account_id,
        to_iban=to_iban,
        amount=amount,
        currency=currency,
        description=description,
    )


# ── Payment creation ───────────────────────────────────────────────────────────


class TestCreatePayment:
    @pytest.mark.asyncio
    async def test_new_payment_is_persisted_in_pending_state(
        self, db_session, mock_redis
    ):
        # GIVEN a valid payment request
        request = _make_request()
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN the payment is created
        result = await service.create_payment(request)

        # THEN the returned response reflects a PENDING payment with correct fields
        assert result.state == PaymentState.PENDING
        assert result.external_id == request.external_id
        assert result.amount == Decimal("42.50")
        assert result.currency == "EUR"
        assert result.to_iban == "NL02ABNA0123456789"
        assert result.id is not None

    @pytest.mark.asyncio
    async def test_payment_creation_also_writes_outbox_record(
        self, db_session, mock_redis
    ):
        # GIVEN a valid payment request
        request = _make_request()
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN the payment is created
        payment = await service.create_payment(request)

        # THEN exactly one PENDING outbox record exists for this payment
        result = await db_session.execute(
            select(Outbox).where(Outbox.payment_id == payment.id)
        )
        outbox = result.scalar_one_or_none()
        assert outbox is not None
        assert outbox.status == OutboxStatus.PENDING
        assert outbox.payload["external_id"] == request.external_id

    @pytest.mark.asyncio
    async def test_to_iban_is_normalised_to_uppercase(self, db_session, mock_redis):
        # GIVEN a request with a lowercase IBAN
        request = _make_request(
            external_id="iban-norm-test", to_iban="nl02abna0123456789"
        )
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN created
        result = await service.create_payment(request)

        # THEN the stored IBAN is normalised to uppercase
        assert result.to_iban == "NL02ABNA0123456789"

    @pytest.mark.asyncio
    async def test_currency_is_normalised_to_uppercase(self, db_session, mock_redis):
        # GIVEN a request with a lowercase currency code
        request = _make_request(external_id="currency-norm-test", currency="eur")
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN created
        result = await service.create_payment(request)

        # THEN the stored currency is normalised to uppercase
        assert result.currency == "EUR"

    @pytest.mark.asyncio
    async def test_two_distinct_external_ids_create_two_distinct_payments(
        self, db_session, mock_redis
    ):
        # GIVEN two separate payment requests
        service = PaymentService(db=db_session, redis=mock_redis)
        request_a = _make_request(external_id="order-A", amount=Decimal("10.00"))
        request_b = _make_request(external_id="order-B", amount=Decimal("20.00"))

        # WHEN both are created
        payment_a = await service.create_payment(request_a)
        payment_b = await service.create_payment(request_b)

        # THEN they produce distinct payment records
        assert payment_a.id != payment_b.id
        assert payment_a.amount == Decimal("10.00")
        assert payment_b.amount == Decimal("20.00")


# ── Idempotency ────────────────────────────────────────────────────────────────


class TestPaymentIdempotency:
    @pytest.mark.asyncio
    async def test_same_external_id_returns_same_payment(self, db_session, mock_redis):
        # GIVEN a payment that was already created
        service = PaymentService(db=db_session, redis=mock_redis)
        request = _make_request(external_id="idem-test-001")
        first = await service.create_payment(request)

        # WHEN the same request is submitted again
        second = await service.create_payment(request)

        # THEN the same payment is returned (no duplicate created)
        assert second.id == first.id
        assert second.external_id == first.external_id

    @pytest.mark.asyncio
    async def test_redis_cache_is_populated_after_first_create(
        self, db_session, mock_redis, redis_store
    ):
        # GIVEN no prior payments
        service = PaymentService(db=db_session, redis=mock_redis)
        request = _make_request(external_id="cache-populate-test")

        # WHEN a payment is created
        payment = await service.create_payment(request)

        # THEN the Redis idempotency cache holds the payment ID
        cache_key = f"idem:{request.external_id}"
        cached_id = await mock_redis.get(cache_key)
        assert cached_id == str(payment.id)

    @pytest.mark.asyncio
    async def test_redis_cache_fast_path_avoids_duplicate_db_write(
        self, db_session, mock_redis
    ):
        # GIVEN a payment that was already created (and is therefore cached)
        service = PaymentService(db=db_session, redis=mock_redis)
        request = _make_request(external_id="cache-hit-test")
        first = await service.create_payment(request)

        # WHEN submitted again (will hit the Redis fast path)
        second = await service.create_payment(request)

        # THEN exactly one outbox record exists — the cache prevented a second write
        result = await db_session.execute(
            select(Outbox).where(Outbox.payment_id == first.id)
        )
        outbox_records = result.scalars().all()
        assert len(outbox_records) == 1
        assert second.id == first.id

    @pytest.mark.asyncio
    async def test_concurrent_identical_requests_do_not_double_create(
            self, async_engine, mock_redis
        ):
            # GIVEN two concurrent requests with the same external_id, each on
            # its own session (AsyncSession is not concurrency-safe; sharing one
            # session across concurrent coroutines corrupts async connection state).
            from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

            factory = async_sessionmaker(
                bind=async_engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )
            request = _make_request(external_id="concurrent-idem-test")

            async def _call():
                async with factory() as session:
                    service = PaymentService(db=session, redis=mock_redis)
                    result = await service.create_payment(request)
                    await session.commit()
                    return result

            # WHEN both fire concurrently on independent sessions
            results = await asyncio.gather(
                _call(),
                _call(),
                return_exceptions=True,
            )

            # THEN at most one unique payment ID is produced
            payment_ids = set()
            for r in results:
                if isinstance(r, Exception):
                    assert isinstance(r, (IdempotencyLockError, DuplicatePaymentError)), (
                        f"Unexpected exception type: {type(r).__name__}: {r}"
                    )
                else:
                    payment_ids.add(r.id)
            assert len(payment_ids) <= 1, (
                "Concurrent duplicate requests must produce at most one payment"
            )

# ── Read operations ────────────────────────────────────────────────────────────


class TestGetPayment:
    @pytest.mark.asyncio
    async def test_get_returns_correct_payment(self, db_session, mock_redis):
        # GIVEN a persisted payment (via factory, not direct ORM)
        payment = await PaymentFactory.create(db_session, amount=Decimal("99.99"))
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN fetched by ID
        result = await service.get_payment(payment.id)

        # THEN the correct payment is returned
        assert result.id == payment.id
        assert result.amount == Decimal("99.99")

    @pytest.mark.asyncio
    async def test_get_nonexistent_payment_raises_not_found(
        self, db_session, mock_redis
    ):
        # GIVEN no payments exist
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN fetching a random UUID
        # THEN PaymentNotFoundError is raised
        with pytest.raises(PaymentNotFoundError):
            await service.get_payment(uuid.uuid4())


class TestListPayments:
    @pytest.mark.asyncio
    async def test_list_returns_all_persisted_payments(self, db_session, mock_redis):
        # GIVEN three payments persisted via factory
        for _ in range(3):
            await PaymentFactory.create(db_session)
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN listed
        result = await service.list_payments(page=1, page_size=10)

        # THEN all three are returned
        assert result.total == 3
        assert len(result.items) == 3

    @pytest.mark.asyncio
    async def test_pagination_returns_non_overlapping_pages(
        self, db_session, mock_redis
    ):
        # GIVEN five payments
        for _ in range(5):
            await PaymentFactory.create(db_session)
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN listed with page_size=2
        page1 = await service.list_payments(page=1, page_size=2)
        page2 = await service.list_payments(page=2, page_size=2)

        # THEN pages don't overlap and the total is correct
        assert page1.total == 5
        assert len(page1.items) == 2
        assert len(page2.items) == 2
        ids_p1 = {p.id for p in page1.items}
        ids_p2 = {p.id for p in page2.items}
        assert ids_p1.isdisjoint(ids_p2), "Pages must not contain overlapping payments"

    @pytest.mark.asyncio
    async def test_list_with_state_filter_returns_only_matching_payments(
        self, db_session, mock_redis
    ):
        # GIVEN a mix of PENDING and CONFIRMED payments
        await PaymentFactory.create(db_session, state=PaymentState.PENDING)
        await PaymentFactory.create(db_session, state=PaymentState.PENDING)
        await PaymentFactory.create(db_session, state=PaymentState.CONFIRMED)
        service = PaymentService(db=db_session, redis=mock_redis)

        # WHEN filtered by state=PENDING
        result = await service.list_payments(state=PaymentState.PENDING)

        # THEN only PENDING payments are returned
        assert result.total == 2
        assert all(p.state == PaymentState.PENDING for p in result.items)
