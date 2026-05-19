"""
tests/integration/webhooks/test_webhook_processor.py — Integration tests for WebhookProcessor.

Tests the receive() and process() paths including deduplication, raw storage,
Redis enqueuing, and domain state-machine dispatch.

Architecture:
  - GIVEN  : test data (payments, webhook events, dedup records) is created
             exclusively via factories.
  - WHEN   : only public WebhookProcessor methods (receive, process, mark_queued)
             are called.
  - THEN   : assertions target DB state (via db_session queries) or the service
             return value. Redis queue depth is checked via the WebhookQueue API.
"""

from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.constants import PaymentState, WebhookEventStatus
from app.core.exceptions import DuplicateWebhookError
from app.domain.webhooks.models import ProcessedWebhookEvent, WebhookEvent
from app.domain.webhooks.processor import WebhookProcessor
from app.infrastructure.messaging.queue import WebhookQueue
from tests.factories import (
    PaymentFactory,
    ProcessedWebhookEventFactory,
    WebhookEventFactory,
)


# ── receive() ─────────────────────────────────────────────────────────────────


class TestWebhookReceive:
    @pytest.mark.asyncio
    async def test_received_event_is_persisted_with_received_status(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        # GIVEN a valid incoming PAYMENT webhook payload
        payload = bunq_payment_webhook_factory()
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the event is received
        await processor.receive(
            event_id="evt-persist-001",
            payload=payload,
            raw_body=b'{"raw": true}',
        )
        await db_session.flush()

        # THEN the event is stored with RECEIVED status and the correct raw body
        result = await db_session.execute(
            select(WebhookEvent).where(WebhookEvent.event_id == "evt-persist-001")
        )
        stored = result.scalar_one_or_none()
        assert stored is not None
        assert stored.raw_body == '{"raw": true}'
        assert stored.status == WebhookEventStatus.RECEIVED

    @pytest.mark.asyncio
    async def test_mark_queued_pushes_event_to_redis_queue(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        # GIVEN a received (but not yet queued) webhook event
        payload = bunq_payment_webhook_factory()
        processor = WebhookProcessor(db=db_session, redis=mock_redis)
        event = await processor.receive(
            event_id="evt-enqueue-001",
            payload=payload,
            raw_body=b"{}",
        )
        await db_session.flush()

        # WHEN mark_queued is called (as the route handler does after DB commit)
        await processor.mark_queued(event)
        await db_session.flush()

        # THEN the queue contains exactly one item and the event is QUEUED
        queue = WebhookQueue(mock_redis)
        assert await queue.length() == 1
        assert event.status == WebhookEventStatus.QUEUED

    @pytest.mark.asyncio
    async def test_receiving_already_processed_event_id_raises_duplicate_error(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        # GIVEN an event_id that is already in the deduplication table
        await ProcessedWebhookEventFactory.create(
            db_session, event_id="evt-dup-recv-001"
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the same event_id arrives again
        # THEN DuplicateWebhookError is raised
        with pytest.raises(DuplicateWebhookError) as exc_info:
            await processor.receive(
                event_id="evt-dup-recv-001",
                payload=bunq_payment_webhook_factory(),
                raw_body=b"{}",
            )
        assert "evt-dup-recv-001" in str(exc_info.value)


# ── process() ─────────────────────────────────────────────────────────────────


class TestWebhookProcess:
    @pytest.mark.asyncio
    async def test_processing_marks_event_as_processed(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        # GIVEN a queued webhook event in the DB
        event = await WebhookEventFactory.create_queued(
            db_session,
            event_id="evt-process-mark-001",
            payload=bunq_payment_webhook_factory(),
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the event is processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the event is PROCESSED with a non-null processed_at timestamp
        await db_session.refresh(event)
        assert event.status == WebhookEventStatus.PROCESSED
        assert event.processed_at is not None

    @pytest.mark.asyncio
    async def test_processing_writes_deduplication_record(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        # GIVEN a queued webhook event
        event = await WebhookEventFactory.create_queued(
            db_session,
            event_id="evt-dedup-write-001",
            payload=bunq_payment_webhook_factory(),
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN a deduplication record exists for this event_id
        result = await db_session.execute(
            select(ProcessedWebhookEvent).where(
                ProcessedWebhookEvent.event_id == "evt-dedup-write-001"
            )
        )
        assert result.scalar_one_or_none() is not None

    @pytest.mark.asyncio
    async def test_already_processed_event_is_skipped_at_worker_level(
        self, db_session, mock_redis, bunq_payment_webhook_factory
    ):
        """
        Defence-in-depth: if an event appears in processed_webhook_events but the
        webhook_events row was not updated (e.g. crash between the two writes),
        the worker must skip it and mark it SKIPPED — not process it again.
        """
        # GIVEN a webhook event that is queued but whose event_id is already in the
        # deduplication table (simulates a crash after dedup write but before status update)
        event = await WebhookEventFactory.create_queued(
            db_session,
            event_id="evt-worker-dup-001",
            payload=bunq_payment_webhook_factory(),
        )
        await ProcessedWebhookEventFactory.create(
            db_session,
            event_id="evt-worker-dup-001",
            webhook_event=event,
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the worker tries to process it
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the event is marked SKIPPED, not PROCESSED
        await db_session.refresh(event)
        assert event.status == WebhookEventStatus.SKIPPED


# ── Domain dispatch: PAYMENT events ───────────────────────────────────────────


class TestPaymentWebhookDispatch:
    @pytest.mark.asyncio
    async def test_accepted_webhook_advances_submitted_payment_to_confirmed(
        self, db_session, mock_redis
    ):
        # GIVEN a payment in SUBMITTED state with a known bunq_payment_id
        payment = await PaymentFactory.create_submitted(
            db_session,
            bunq_payment_id="99001",
            amount=Decimal("42.50"),
            currency="EUR",
            from_account_id="acct-confirm-dispatch",
        )
        # AND a queued PAYMENT webhook for that bunq payment with status=ACCEPTED
        event = await WebhookEventFactory.create_for_payment(
            db_session,
            bunq_payment_id=99001,
            bunq_status="ACCEPTED",
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the webhook is processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the payment is CONFIRMED with a confirmed_at timestamp
        await db_session.refresh(payment)
        assert payment.state == PaymentState.CONFIRMED
        assert payment.confirmed_at is not None

    @pytest.mark.asyncio
    async def test_rejected_webhook_advances_submitted_payment_to_failed(
        self, db_session, mock_redis
    ):
        # GIVEN a payment in SUBMITTED state
        payment = await PaymentFactory.create_submitted(
            db_session,
            bunq_payment_id="99002",
            amount=Decimal("10.00"),
            currency="EUR",
            from_account_id="acct-fail-dispatch",
        )
        # AND a REJECTED webhook
        event = await WebhookEventFactory.create_for_payment(
            db_session,
            bunq_payment_id=99002,
            bunq_status="REJECTED",
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the payment transitions to FAILED
        await db_session.refresh(payment)
        assert payment.state == PaymentState.FAILED

    @pytest.mark.asyncio
    async def test_out_of_order_accepted_webhook_does_not_regress_confirmed_payment(
        self, db_session, mock_redis
    ):
        """
        A late-arriving ACCEPTED webhook for an already-CONFIRMED payment must not
        attempt to re-confirm it or roll back the state.
        """
        # GIVEN a payment already in CONFIRMED state
        payment = await PaymentFactory.create_confirmed(
            db_session,
            bunq_payment_id="77001",
            amount=Decimal("10.00"),
            currency="EUR",
            from_account_id="acct-ooo-dispatch",
        )
        # AND a late ACCEPTED webhook for the same bunq payment
        event = await WebhookEventFactory.create_for_payment(
            db_session,
            bunq_payment_id=77001,
            bunq_status="ACCEPTED",
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN the stale webhook is processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the payment remains CONFIRMED — no state regression
        await db_session.refresh(payment)
        assert payment.state == PaymentState.CONFIRMED

    @pytest.mark.asyncio
    async def test_webhook_for_unknown_bunq_payment_id_is_processed_without_error(
        self, db_session, mock_redis
    ):
        """
        Webhooks referencing a bunq_payment_id not in our DB (e.g. payments
        initiated outside this system) must be processed gracefully — logged
        but not failed.
        """
        # GIVEN a PAYMENT webhook referencing a bunq_payment_id we have no record of
        event = await WebhookEventFactory.create_for_payment(
            db_session,
            bunq_payment_id=999999,  # No matching Payment row exists
        )
        processor = WebhookProcessor(db=db_session, redis=mock_redis)

        # WHEN processed
        await processor.process(str(event.id))
        await db_session.flush()

        # THEN the event is marked PROCESSED (not FAILED)
        await db_session.refresh(event)
        assert event.status == WebhookEventStatus.PROCESSED
