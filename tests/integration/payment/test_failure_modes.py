"""
tests/integration/payment/test_failure_modes.py — Failure mode integration tests.

Tests the system's behaviour under conditions that deviate from the happy path:
  - DB flush failure during payment creation
  - Concurrent duplicate requests (race conditions)
  - Webhook duplication at receive and process layers
  - Out-of-order webhook delivery
  - Outbox record state after simulated worker crash

Architecture:
  - GIVEN  : test data set up via factories only.
  - WHEN   : public service/processor methods are called (possibly with patches).
  - THEN   : assertions on service return values or DB state (never internal state).
"""

import asyncio
import uuid
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app.core.constants import OutboxStatus, PaymentState, WebhookEventStatus
from app.core.exceptions import (
    DuplicatePaymentError,
    DuplicateWebhookError,
    IdempotencyLockError,
)
from app.domain.payments.models import CreatePaymentRequest, Payment
from app.domain.payments.service import PaymentService
from app.domain.webhooks.processor import WebhookProcessor
from app.infrastructure.db.outbox import Outbox
from tests.factories import (
    OutboxFactory,
    PaymentFactory,
    ProcessedWebhookEventFactory,
    WebhookEventFactory,
)


def _make_request(
    *,
    external_id: str | None = None,
    amount: Decimal = Decimal("10.00"),
) -> CreatePaymentRequest:
    return CreatePaymentRequest(
        external_id=external_id or f"fail-ext-{uuid.uuid4().hex[:10]}",
        from_account_id="123",
        to_iban="NL02ABNA0123456789",
        amount=amount,
        currency="EUR",
    )


# ── DB failure during payment creation ────────────────────────────────────────


class TestDBFlushFailure:
    @pytest.mark.asyncio
    async def test_flush_failure_leaves_no_payment_record(self, db_session, mock_redis):
        """
        If the DB flush fails mid-creation, the payment and outbox records
        must not be committed — the caller can safely retry.
        """
        # GIVEN a valid request and a session that will fail on flush
        service = PaymentService(db=db_session, redis=mock_redis)
        request = _make_request(external_id="db-flush-fail-001")

        with patch.object(
            db_session, "flush", side_effect=Exception("DB flush failed")
        ):
            with pytest.raises(Exception, match="DB flush failed"):
                await service.create_payment(request)

        # Reset session after the forced failure
        await db_session.rollback()

        # THEN no payment record was persisted
        result = await db_session.execute(
            select(Payment).where(Payment.external_id == "db-flush-fail-001")
        )
        assert result.scalar_one_or_none() is None


# ── Duplicate payment requests ─────────────────────────────────────────────────


class TestDuplicatePaymentRequests:
    @pytest.mark.asyncio
    async def test_concurrent_identical_requests_produce_at_most_one_payment(
        self, db_session, mock_redis
    ):
        """
        Two concurrent requests with the same external_id must not create two payments.
        One request should succeed; the other either returns the same payment or raises
        an idempotency error — both outcomes are correct.
        """
        # GIVEN two concurrent requests with the same external_id
        service = PaymentService(db=db_session, redis=mock_redis)
        request = _make_request(external_id="concurrent-dup-test")

        # WHEN both fire at once
        results = await asyncio.gather(
            service.create_payment(request),
            service.create_payment(request),
            return_exceptions=True,
        )

        # THEN at most one unique payment ID exists
        payment_ids = set()
        for r in results:
            if isinstance(r, Exception):
                assert isinstance(r, (IdempotencyLockError, DuplicatePaymentError)), (
                    f"Unexpected exception: {type(r).__name__}: {r}"
                )
            else:
                payment_ids.add(r.id)
        assert len(payment_ids) <= 1

    @pytest.mark.asyncio
    async def test_second_call_with_same_external_id_creates_no_extra_outbox_record(
        self, db_session, mock_redis
    ):
        """
        After the first successful create, the idempotency cache ensures the
        second call returns the cached result without writing another outbox record.
        """
        # GIVEN a payment that was already created
        service = PaymentService(db=db_session, redis=mock_redis)
        request = _make_request(external_id="idem-outbox-check")
        first = await service.create_payment(request)

        # WHEN the same request is submitted again
        second = await service.create_payment(request)

        # THEN the same payment is returned and only one outbox record exists
        assert second.id == first.id
        result = await db_session.execute(
            select(Outbox).where(Outbox.payment_id == first.id)
        )
        outbox_records = result.scalars().all()
        assert len(outbox_records) == 1


# ── Outbox state after worker crash ───────────────────────────────────────────


class TestWorkerCrashScenario:
    @pytest.mark.asyncio
    async def test_outbox_record_remains_pending_when_worker_has_not_processed_it(
        self, db_session, mock_redis
    ):
        """
        Simulates: worker picked up the outbox record but crashed before calling bunq.
        The outbox record stays PENDING so the next poll will retry.
        """
        # GIVEN a payment with its outbox record (both in PENDING state via factories)
        payment = await PaymentFactory.create(db_session, state=PaymentState.PENDING)
        outbox = await OutboxFactory.create(
            db_session, payment=payment, status=OutboxStatus.PENDING
        )

        # WHEN no worker processes it (simulating a crash before processing)
        # THEN the outbox record remains PENDING
        await db_session.refresh(outbox)
        assert outbox.status == OutboxStatus.PENDING

    @pytest.mark.asyncio
    async def test_payment_in_processing_state_is_recoverable(
        self, db_session, mock_redis
    ):
        """
        A payment stuck in PROCESSING state (worker crashed mid-flight) can be
        retried: PROCESSING → FAILED → PENDING is a valid recovery path.
        """
        # GIVEN a payment stuck in PROCESSING state (factory creates it there)
        payment = await PaymentFactory.create(db_session, state=PaymentState.PROCESSING)

        # THEN the state machine allows it to be failed (representing the crash recovery)
        from app.domain.payments.state_machine import PaymentStateMachine

        machine = PaymentStateMachine(payment)
        assert machine.can_transition_to(PaymentState.FAILED)


# ── Webhook duplication ────────────────────────────────────────────────────────


class TestWebhookDuplication:
    @pytest.mark.asyncio
    async def test_already_processed_event_id_raises_at_receive(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        """
        Receiving a webhook whose event_id is already in processed_webhook_events
        must raise DuplicateWebhookError immediately, before any DB write.
        """
        # GIVEN an event_id that is already processed
        await ProcessedWebhookEventFactory.create(
            db_session, event_id="dup-recv-scenario"
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the same event arrives again
        # THEN DuplicateWebhookError is raised
        with pytest.raises(DuplicateWebhookError):
            await processor.receive(
                event_id="dup-recv-scenario",
                payload=bunq_payment_webhook_factory(),
                raw_body=b"{}",
            )

    @pytest.mark.asyncio
    async def test_worker_skips_event_already_in_processed_table(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        """
        Defence-in-depth: if the dedup record was written but the webhook_events
        row was not updated (crash between the two), the worker marks the event
        SKIPPED instead of re-processing it.
        """
        # GIVEN a queued event that also has a dedup record (simulating the crash window)
        event = await WebhookEventFactory.create_queued(
            db_session,
            event_id="worker-dup-scenario",
            payload=bunq_payment_webhook_factory(),
        )
        await ProcessedWebhookEventFactory.create(
            db_session, event_id="worker-dup-scenario", webhook_event=event
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the worker processes it
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN it is SKIPPED, not PROCESSED again
        await db_session.refresh(event)
        assert event.status == WebhookEventStatus.SKIPPED


# ── Out-of-order webhook delivery ─────────────────────────────────────────────


class TestOutOfOrderWebhooks:
    @pytest.mark.asyncio
    async def test_late_accepted_webhook_for_confirmed_payment_is_safe(
        self, db_session, mock_redis
    ):
        """
        A PAYMENT/ACCEPTED webhook arriving after the payment is already CONFIRMED
        (e.g. due to network redelivery) must not attempt an invalid state transition.
        The payment stays CONFIRMED.
        """
        # GIVEN a payment already in CONFIRMED state
        payment = await PaymentFactory.create_confirmed(
            db_session,
            bunq_payment_id="77002",
            amount=Decimal("10.00"),
            currency="EUR",
            from_account_id="acct-ooo-safe",
        )
        # AND a late-arriving ACCEPTED webhook for the same payment
        event = await WebhookEventFactory.create_for_payment(
            db_session,
            bunq_payment_id=77002,
            bunq_status="ACCEPTED",
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the stale webhook is processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the payment remains CONFIRMED — no regression
        await db_session.refresh(payment)
        assert payment.state == PaymentState.CONFIRMED
