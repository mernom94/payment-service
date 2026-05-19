"""
app/domain/webhooks/processor.py — Webhook receive and domain dispatch.

Two responsibilities:
  1. WebhookProcessor.receive() — called by the API route to store and enqueue.
  2. WebhookProcessor.process() — called by the webhook worker to apply domain effects.

Separation ensures the API route can return 200 immediately (receive), while
the potentially-slow domain work (process) runs asynchronously.

Design note on enqueue ordering (issue #2 fix):
  The Redis enqueue happens AFTER the DB transaction commits, not before.
  The HTTP handler commits via get_db on exit; receive() no longer calls
  enqueue directly.  Instead, it stores the event and returns; the route
  handler enqueues after the commit succeeds.  This eliminates the window
  where a Redis push could reference an event that never committed.

  See app/api/routes/webhooks.py for the two-phase call pattern.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import PaymentState, WebhookEventStatus
from app.core.exceptions import DuplicateWebhookError
from app.domain.ledger.engine import LedgerEngine
from app.domain.payments.models import Payment
from app.domain.payments.state_machine import PaymentStateMachine
from app.domain.webhooks.models import ProcessedWebhookEvent, WebhookEvent
from app.infrastructure.messaging.queue import WebhookQueue

logger = logging.getLogger(__name__)


class WebhookProcessor:
    def __init__(self, db: AsyncSession, redis) -> None:
        self._db = db
        self._redis = redis
        self._queue = WebhookQueue(redis)
        self._ledger = LedgerEngine(db)

    # ── Called by API route (fast path) ──────────────────────────────────────

    async def receive(
        self,
        *,
        event_id: str,
        payload: dict[str, Any],
        raw_body: bytes,
    ) -> WebhookEvent:
        """
        Store the raw event and return it so the caller can enqueue after commit.

        The caller (API route) must enqueue the event ID AFTER the DB
        transaction commits.  This prevents a Redis push from referencing an
        event that never made it to the database.

        Deduplication is enforced by a UNIQUE constraint on webhook_events.event_id
        (DB-level) in addition to the application-level check.  The IntegrityError
        path handles the race between two concurrent requests with the same event_id.
        """
        # Application-level check (fast path before DB write).
        # Uses the pre-processing table (webhook_events) so we detect events
        # that have been stored but not yet processed — e.g. bunq retrying
        # before our worker has consumed the first delivery.
        already_received = await self._is_receive_duplicate(event_id)
        if already_received:
            logger.info(
                "webhook.receive.duplicate",
                extra={"event_id": event_id},
            )
            raise DuplicateWebhookError(event_id)

        event = WebhookEvent(
            id=uuid.uuid4(),
            event_id=event_id,
            event_type=payload.get("NotificationType"),
            payload=payload,
            raw_body=raw_body.decode(errors="replace"),
            status=WebhookEventStatus.RECEIVED,
        )
        self._db.add(event)

        try:
            await self._db.flush()
        except IntegrityError:
            # DB-level UNIQUE constraint on event_id caught a concurrent insert.
            await self._db.rollback()
            logger.info(
                "webhook.receive.duplicate_constraint",
                extra={"event_id": event_id},
            )
            raise DuplicateWebhookError(event_id)

        # Do NOT enqueue here — the caller must enqueue after the commit.
        # Setting status to RECEIVED (not QUEUED) until the caller confirms.
        logger.info(
            "webhook.receive.stored",
            extra={"event_id": event_id, "webhook_event_id": str(event.id)},
        )
        return event

    async def mark_queued(self, event: WebhookEvent) -> None:
        """
        Mark the event as QUEUED and push it to Redis.

        Must be called AFTER the DB transaction that persisted the event has
        committed.  Splitting receive() and mark_queued() ensures Redis never
        references an uncommitted event.
        """
        await self._queue.enqueue(str(event.id))
        event.status = WebhookEventStatus.QUEUED
        await self._db.flush()
        logger.info(
            "webhook.receive.queued",
            extra={"event_id": event.event_id, "webhook_event_id": str(event.id)},
        )

    # ── Called by webhook worker (async path) ────────────────────────────────

    async def process(self, webhook_event_id: str) -> None:
        """
        Process a queued webhook event.

        Steps:
          1. Load the WebhookEvent.
          2. Check deduplication (worker-level — defence in depth).
          3. Dispatch to the appropriate domain handler.
          4. Mark as processed + write deduplication record atomically.

        Note: this method calls db.flush() throughout but NEVER db.commit().
        The webhook worker is responsible for committing after this returns,
        ensuring all state changes are atomic with the caller's transaction.
        """
        result = await self._db.execute(
            select(WebhookEvent).where(WebhookEvent.id == uuid.UUID(webhook_event_id))
        )
        event = result.scalar_one_or_none()
        if not event:
            logger.warning(
                "webhook.process.not_found",
                extra={"webhook_event_id": webhook_event_id},
            )
            return

        # Defence-in-depth deduplication at worker level.
        # Checks processed_webhook_events (the post-processing table) — correct
        # here because we want to know if processing already completed, not just
        # whether the event was received.
        if await self._is_process_duplicate(event.event_id):
            logger.info(
                "webhook.process.already_processed", extra={"event_id": event.event_id}
            )
            event.status = WebhookEventStatus.SKIPPED
            await self._db.flush()
            return

        try:
            await self._dispatch(event)

            # Mark processed + write dedup record in same transaction.
            event.status = WebhookEventStatus.PROCESSED
            event.processed_at = datetime.now(timezone.utc)

            dedup = ProcessedWebhookEvent(
                event_id=event.event_id,
                webhook_event_id=event.id,
            )
            self._db.add(dedup)
            await self._db.flush()

            logger.info(
                "webhook.process.done",
                extra={"event_id": event.event_id, "event_type": event.event_type},
            )

        except Exception as exc:
            event.status = WebhookEventStatus.FAILED
            event.error_message = str(exc)
            await self._db.flush()
            logger.error(
                "webhook.process.failed",
                extra={"event_id": event.event_id, "error": str(exc)},
                exc_info=True,
            )
            raise

    # ── Domain dispatch ───────────────────────────────────────────────────────

    async def _dispatch(self, event: WebhookEvent) -> None:
        """Route the event to the appropriate domain handler."""
        event_type = (event.event_type or "").upper()
        payload = event.payload if isinstance(event.payload, dict) else {}

        if event_type == "PAYMENT":
            await self._handle_payment_event(event, payload)
        else:
            logger.info(
                "webhook.dispatch.unhandled_type",
                extra={"event_type": event_type, "event_id": event.event_id},
            )

    async def _handle_payment_event(
        self, event: WebhookEvent, payload: dict[str, Any]
    ) -> None:
        """
        Handle a PAYMENT webhook from bunq.

        Looks up the internal payment by bunq_payment_id, then advances the
        state machine and writes the confirmation ledger entry.
        """
        payment_obj = payload.get("Payment", {})
        bunq_payment_id = str(payment_obj.get("id", ""))
        status = payment_obj.get("status", "").upper()

        if not bunq_payment_id:
            logger.warning("webhook.payment.no_id", extra={"payload": payload})
            return

        result = await self._db.execute(
            select(Payment).where(Payment.bunq_payment_id == bunq_payment_id)
        )
        payment = result.scalar_one_or_none()

        if not payment:
            logger.info(
                "webhook.payment.no_internal_match",
                extra={"bunq_payment_id": bunq_payment_id},
            )
            return

        machine = PaymentStateMachine(payment)

        if status in ("PENDING", "ACCEPTED"):
            if machine.can_transition_to(PaymentState.CONFIRMED):
                machine.transition_to(PaymentState.CONFIRMED)
                await self._ledger.record_payment_confirmed(
                    payment_id=payment.id,
                    from_account_id=payment.from_account_id,
                    amount=payment.amount,
                    currency=payment.currency,
                )
        elif status == "REJECTED":
            if machine.can_transition_to(PaymentState.FAILED):
                machine.transition_to(
                    PaymentState.FAILED,
                    error_message=f"bunq rejected payment: status={status}",
                )
                await self._ledger.record_payment_failed(
                    payment_id=payment.id,
                    from_account_id=payment.from_account_id,
                    amount=payment.amount,
                    currency=payment.currency,
                )
        else:
            logger.info(
                "webhook.payment.unhandled_status",
                extra={"bunq_payment_id": bunq_payment_id, "status": status},
            )

        await self._db.flush()

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _is_receive_duplicate(self, event_id: str) -> bool:
        """
        Receive-path deduplication: check whether a WebhookEvent row already
        exists for this event_id (i.e. the event has been received and stored,
        even if not yet processed).

        Checking webhook_events (the pre-processing table) rather than
        processed_webhook_events means we catch the case where bunq retries
        before our worker has had a chance to process the first delivery.
        If we checked processed_webhook_events here, a retry that arrives
        before processing completes would pass the check, create a second
        WebhookEvent row, and both would be queued — wasting worker cycles and
        cluttering the table with duplicate rows.
        """
        # Check BOTH tables for full idempotency
        result = await self._db.execute(
            select(WebhookEvent.id).where(WebhookEvent.event_id == event_id)
        )
        if result.scalar_one_or_none():
            return True

        result = await self._db.execute(
            select(ProcessedWebhookEvent.event_id).where(
                ProcessedWebhookEvent.event_id == event_id
            )
        )
        return result.scalar_one_or_none() is not None

    async def _is_process_duplicate(self, event_id: str) -> bool:
        """
        Worker-path deduplication: check whether a ProcessedWebhookEvent row
        exists for this event_id (i.e. the event has been fully processed).

        This is the correct table to check inside process() because at that
        point we want to know whether processing already completed, not merely
        whether the event was received.
        """
        result = await self._db.execute(
            select(ProcessedWebhookEvent).where(
                ProcessedWebhookEvent.event_id == event_id
            )
        )
        return result.scalar_one_or_none() is not None
