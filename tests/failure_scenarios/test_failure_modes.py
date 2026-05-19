"""
tests/failure_scenarios/test_failure_modes.py

Production-grade failure-mode integration tests for the payment system.

Goals
-----
These tests validate the system's behaviour under:
  - concurrent duplicate requests
  - webhook duplication/reordering
  - DB failures
  - worker crashes
  - ambiguous external provider outcomes

Testing Principles
------------------
1. Never share AsyncSession objects across concurrent tasks.
2. Every concurrent flow gets its own DB session.
3. Tests verify recoverable system state rather than implementation detail.
4. Assertions focus on invariants:
     - no duplicate payments
     - idempotency correctness
     - state machine safety
     - durable outbox guarantees
5. Explicit commits are used where durability matters.
"""

import asyncio
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from app.core.constants import OutboxStatus, PaymentState
from app.core.exceptions import (
    BunqPaymentAmbiguousError,
    DuplicatePaymentError,
    DuplicateWebhookError,
    IdempotencyLockError,
)
from app.domain.payments.models import CreatePaymentRequest, Payment
from app.domain.payments.service import PaymentService
from app.domain.webhooks.models import (
    ProcessedWebhookEvent,
    WebhookEvent,
)
from app.domain.webhooks.processor import WebhookProcessor
from app.infrastructure.db.outbox import Outbox


# ============================================================================
# Concurrent duplicate payment requests
# ============================================================================

class TestDuplicatePaymentRequests:
    @pytest.mark.asyncio
    async def test_concurrent_identical_requests_return_same_payment(
        self, async_engine, mock_redis
    ):
        """
        Simulate two concurrent requests with the same external_id arriving on
        separate connections, as in production with multiple HTTP workers.

        SQLAlchemy AsyncSession is NOT concurrency-safe: two coroutines sharing
        one session will corrupt connection state under real async PostgreSQL.
        This test gives each concurrent call its own session, matching the
        production pattern where each request gets a dedicated session via
        get_db().

        Either both calls succeed (idempotent — same payment returned), or one
        raises IdempotencyLockError/DuplicatePaymentError. Both are correct:
        the invariant is that at most ONE payment row is written.
        """
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        factory = async_sessionmaker(
            bind=async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )

        request = CreatePaymentRequest(
            external_id="concurrent-test",
            from_account_id="123",
            to_iban="NL02ABNA0123456789",
            amount=Decimal("10.00"),
            currency="EUR",
        )

        async def _call():
            async with factory() as session:
                service = PaymentService(db=session, redis=mock_redis)
                result = await service.create_payment(request)
                await session.commit()
                return result

        # Two genuinely concurrent calls on independent sessions/connections.
        results = await asyncio.gather(
            _call(),
            _call(),
            return_exceptions=True,
        )

        payment_ids = set()
        for r in results:
            if isinstance(r, Exception):
                assert isinstance(r, (IdempotencyLockError, DuplicatePaymentError)), (
                    f"Unexpected exception: {type(r).__name__}: {r}"
                )
            else:
                payment_ids.add(r.id)

        assert len(payment_ids) <= 1, (
            "Concurrent duplicate requests must produce at most one payment"
        )



    @pytest.mark.asyncio
    async def test_idempotency_cache_prevents_second_db_hit(
        self,
        db_session,
        mock_redis,
    ):
        """
        Second request with same external_id should be served from cache
        and must not create duplicate outbox records.
        """

        service = PaymentService(
            db=db_session,
            redis=mock_redis,
        )

        request = CreatePaymentRequest(
            external_id="cache-test-001",
            from_account_id="123",
            to_iban="NL02ABNA0123456789",
            amount=Decimal("5.00"),
            currency="EUR",
        )

        first = await service.create_payment(request)
        second = await service.create_payment(request)

        assert first.id == second.id

        result = await db_session.execute(
            select(Outbox).where(Outbox.payment_id == first.id)
        )

        outbox_records = result.scalars().all()

        assert len(outbox_records) == 1


# ============================================================================
# Worker crash scenarios
# ============================================================================


class TestWorkerCrashMidPayment:
    @pytest.mark.asyncio
    async def test_outbox_record_remains_pending_if_worker_crashes_before_bunq(
        self,
        db_session,
        mock_redis,
    ):
        """
        If the worker crashes before external dispatch, the outbox row
        must remain retryable.
        """

        service = PaymentService(
            db=db_session,
            redis=mock_redis,
        )

        payment = await service.create_payment(
            CreatePaymentRequest(
                external_id="crash-before-bunq",
                from_account_id="123",
                to_iban="NL02ABNA0123456789",
                amount=Decimal("10.00"),
                currency="EUR",
            )
        )

        await db_session.commit()

        result = await db_session.execute(
            select(Outbox).where(Outbox.payment_id == payment.id)
        )

        outbox = result.scalar_one()

        assert outbox.status == OutboxStatus.PENDING

    @pytest.mark.asyncio
    async def test_ambiguous_payment_flagged_after_timeout(
        self,
        db_session,
        mock_redis,
    ):
        """
        If bunq times out after request submission, the payment outcome
        becomes ambiguous and must be marked FAILED with reconciliation
        metadata preserved.
        """

        from app.workers.payment_worker import PaymentWorker

        service = PaymentService(
            db=db_session,
            redis=mock_redis,
        )

        payment_response = await service.create_payment(
            CreatePaymentRequest(
                external_id="timeout-after-send",
                from_account_id="123",
                to_iban="NL02ABNA0123456789",
                amount=Decimal("100.00"),
                currency="EUR",
            )
        )

        await db_session.commit()

        session_ctx = MagicMock(
            __aenter__=AsyncMock(return_value=db_session),
            __aexit__=AsyncMock(return_value=False),
        )

        session_factory = MagicMock(return_value=session_ctx)

        with patch("app.workers.payment_worker.BunqClient") as mock_client_class:
            mock_client = AsyncMock()

            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)

            mock_adapter = AsyncMock()

            mock_adapter.create_payment = AsyncMock(
                side_effect=BunqPaymentAmbiguousError("Timeout after POST")
            )

            mock_client_class.return_value = mock_client

            with patch(
                "app.workers.payment_worker.BunqPaymentAdapter",
                return_value=mock_adapter,
            ):
                worker = PaymentWorker(
                    session_factory=session_factory,
                    ledger_session_factory=session_factory,
                )

                await worker.tick()

        result = await db_session.execute(
            select(Payment).where(Payment.id == payment_response.id)
        )

        payment = result.scalar_one()

        assert payment.state == PaymentState.FAILED
        assert "AMBIGUOUS" in (payment.last_error or "")


# ============================================================================
# Webhook duplication and ordering
# ============================================================================


class TestWebhookDuplication:
    @pytest.mark.asyncio
    async def test_duplicate_webhook_at_receive_raises(
        self,
        db_session,
        mock_redis,
        valid_bunq_payment_webhook,
    ):
        """
        Duplicate webhook delivery before processing completion should
        be rejected at receive-time.
        """

        db_session.add(
            WebhookEvent(
                id=uuid.uuid4(),
                event_id="dup-webhook-001",
                event_type="PAYMENT",
                payload=valid_bunq_payment_webhook,
                raw_body="{}",
                status="RECEIVED",
            )
        )

        await db_session.flush()

        processor = WebhookProcessor(
            db=db_session,
            redis=mock_redis,
        )

        with pytest.raises(DuplicateWebhookError):
            await processor.receive(
                event_id="dup-webhook-001",
                payload=valid_bunq_payment_webhook,
                raw_body=b"{}",
            )

    @pytest.mark.asyncio
    async def test_duplicate_webhook_at_process_is_skipped(
        self,
        db_session,
        mock_redis,
        valid_bunq_payment_webhook,
    ):
        """
        Worker-level deduplication should safely SKIP already-processed
        events after crash recovery.
        """

        event = WebhookEvent(
            id=uuid.uuid4(),
            event_id="dup-process-001",
            event_type="PAYMENT",
            payload=valid_bunq_payment_webhook,
            raw_body="{}",
            status="QUEUED",
        )

        db_session.add(event)

        db_session.add(
            ProcessedWebhookEvent(
                event_id="dup-process-001",
                webhook_event_id=event.id,
            )
        )

        await db_session.flush()

        processor = WebhookProcessor(
            db=db_session,
            redis=mock_redis,
        )

        await processor.process(str(event.id))

        await db_session.refresh(event)

        assert event.status == "SKIPPED"

    @pytest.mark.asyncio
    async def test_out_of_order_webhook_does_not_regress_state(
        self,
        db_session,
        mock_redis,
    ):
        """
        Late-arriving ACCEPTED webhook must never regress an already
        CONFIRMED payment.
        """

        payment = Payment(
            id=uuid.uuid4(),
            external_id="ooo-test",
            from_account_id="123",
            to_iban="NL02ABNA0123456789",
            amount=Decimal("10.00"),
            currency="EUR",
            state=PaymentState.CONFIRMED,
            bunq_payment_id="77001",
        )

        db_session.add(payment)

        await db_session.flush()
        await db_session.commit()

        payload = {
            "NotificationType": "PAYMENT",
            "Payment": {
                "id": 77001,
                "status": "ACCEPTED",
                "amount": {
                    "value": "10.00",
                    "currency": "EUR",
                },
                "monetary_account_id": 123,
            },
        }

        processor = WebhookProcessor(
            db=db_session,
            redis=mock_redis,
        )

        await processor.receive(
            event_id="evt-ooo",
            payload=payload,
            raw_body=b"{}",
        )

        result = await db_session.execute(
            select(WebhookEvent).where(WebhookEvent.event_id == "evt-ooo")
        )

        webhook_event = result.scalar_one()

        await processor.process(str(webhook_event.id))

        await db_session.refresh(payment)

        assert payment.state == PaymentState.CONFIRMED


# ============================================================================
# DB failure handling
# ============================================================================


class TestDBCommitFailure:
    @pytest.mark.asyncio
    async def test_rollback_on_db_flush_error_prevents_payment_creation(
        self,
        db_session,
        mock_redis,
    ):
        """
        Flush failure during payment creation must leave the database
        unchanged.
        """

        service = PaymentService(
            db=db_session,
            redis=mock_redis,
        )

        request = CreatePaymentRequest(
            external_id="db-fail-test",
            from_account_id="123",
            to_iban="NL02ABNA0123456789",
            amount=Decimal("10.00"),
            currency="EUR",
        )

        with patch.object(
            db_session,
            "flush",
            side_effect=Exception("DB commit failed"),
        ):
            with pytest.raises(
                Exception,
                match="DB commit failed",
            ):
                await service.create_payment(request)

        await db_session.rollback()

        result = await db_session.execute(
            select(Payment).where(Payment.external_id == "db-fail-test")
        )

        assert result.scalar_one_or_none() is None
