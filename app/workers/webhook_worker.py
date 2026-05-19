"""
app/workers/webhook_worker.py — Webhook queue processor.

Drains the webhook queue and processes each event via WebhookProcessor.process().
Runs continuously with a configurable poll interval.

Retry / dead-letter design:
  - If processing fails, the event is NOT silently dropped.
  - Instead it is set to RETRY_PENDING with an exponential backoff schedule
    (retry_after timestamp) and retry_count is incremented.
  - On each tick a secondary scan picks up RETRY_PENDING events whose
    retry_after <= now, re-enqueues them, and advances their status back
    to QUEUED so the normal processing path picks them up.
  - After MAX_WEBHOOK_RETRIES failures the event is permanently set to FAILED
    (dead letter) and an error is logged for manual inspection.

This replaces the previous "log and drop" behaviour that caused permanent
data loss on transient DB errors during webhook processing.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.core.config import get_settings
from app.core.constants import MAX_WEBHOOK_RETRIES, WebhookEventStatus
from app.core.observability import (
    dlq_entries_total,
    get_tracer,
    webhook_events_processed_total,
)
from app.domain.webhooks.models import WebhookEvent
from app.domain.webhooks.processor import WebhookProcessor
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.messaging.queue import WebhookQueue
from app.infrastructure.messaging.workers import BaseWorker
from app.infrastructure.redis.client import get_redis_client

_tracer = get_tracer(__name__)

logger = logging.getLogger(__name__)


class WebhookWorker(BaseWorker):
    name = "webhook_worker"
    poll_interval = get_settings().WEBHOOK_WORKER_POLL_INTERVAL

    async def tick(self) -> None:
        """
        Two-phase tick:
          1. Re-enqueue any RETRY_PENDING events whose retry_after has passed.
          2. Drain the live queue and process up to WEBHOOK_BATCH_SIZE events.

        Each event gets its own DB transaction so a single bad event
        doesn't roll back an entire batch.
        """
        await self._requeue_retry_pending_events()
        await self._process_queued_events()

    # ── Phase 1: reschedule retries ───────────────────────────────────────────

    async def _requeue_retry_pending_events(self) -> None:
        """
        Find RETRY_PENDING events whose retry_after <= now and push them back
        onto the Redis queue so the normal processing path handles them.
        """
        now = datetime.now(timezone.utc)
        session_factory = get_session_factory()
        redis = await get_redis_client()
        queue = WebhookQueue(redis)

        async with session_factory() as db:
            result = await db.execute(
                select(WebhookEvent)
                .where(
                    WebhookEvent.status == WebhookEventStatus.RETRY_PENDING,
                    WebhookEvent.retry_after <= now,
                )
                .with_for_update(skip_locked=True)
                .limit(get_settings().WEBHOOK_BATCH_SIZE)
            )
            events = result.scalars().all()

            for event in events:
                await queue.enqueue(str(event.id))
                event.status = WebhookEventStatus.QUEUED
                logger.info(
                    "webhook_worker.retry.requeued",
                    extra={
                        "webhook_event_id": str(event.id),
                        "event_id": event.event_id,
                        "retry_count": event.retry_count,
                    },
                )

            if events:
                await db.commit()

    # ── Phase 2: drain the live queue ─────────────────────────────────────────

    async def _process_queued_events(self) -> None:
        """Process up to WEBHOOK_BATCH_SIZE events from the Redis queue."""
        redis = await get_redis_client()
        queue = WebhookQueue(redis)
        session_factory = get_session_factory()

        for _ in range(get_settings().WEBHOOK_BATCH_SIZE):
            event_id = await queue.dequeue(timeout=1)
            if event_id is None:
                break  # Queue is empty — stop early.

            try:
                async with session_factory() as db:
                    with _tracer.start_as_current_span(
                        "webhook_worker.process"
                    ) as span:
                        span.set_attribute("webhook.event_id", event_id)
                        processor = WebhookProcessor(db=db, redis=redis)
                        await processor.process(event_id)
                        await db.commit()

                webhook_events_processed_total.labels(
                    event_type="PAYMENT", outcome="success"
                ).inc()
                logger.debug(
                    "webhook_worker.processed",
                    extra={"webhook_event_id": event_id},
                )

            except Exception as exc:
                logger.error(
                    "webhook_worker.failed",
                    extra={"webhook_event_id": event_id, "error": str(exc)},
                    exc_info=True,
                )
                await self._schedule_retry(event_id, str(exc))

    async def _schedule_retry(self, webhook_event_id: str, error_message: str) -> None:
        """
        Schedule a failed webhook event for retry with exponential backoff,
        or permanently fail it if MAX_WEBHOOK_RETRIES is exhausted.

        This replaces the previous "log and drop" behaviour.
        """
        import uuid as _uuid

        # Parse and validate the UUID before touching the DB.
        try:
            parsed_id = _uuid.UUID(webhook_event_id)
        except ValueError:
            logger.error(
                "webhook_worker.retry.invalid_id",
                extra={"webhook_event_id": webhook_event_id},
            )
            return

        session_factory = get_session_factory()
        async with session_factory() as db:
            result = await db.execute(
                select(WebhookEvent).where(WebhookEvent.id == parsed_id)
            )
            event = result.scalar_one_or_none()
            if not event:
                logger.warning(
                    "webhook_worker.retry.event_not_found",
                    extra={"webhook_event_id": webhook_event_id},
                )
                return

            event.retry_count += 1
            event.error_message = error_message

            if event.retry_count >= MAX_WEBHOOK_RETRIES:
                # Permanently failed — dead letter.
                event.status = WebhookEventStatus.FAILED
                dlq_entries_total.labels(worker="webhook_worker").inc()
                webhook_events_processed_total.labels(
                    event_type="PAYMENT", outcome="dead_letter"
                ).inc()
                logger.error(
                    "webhook_worker.dead_letter",
                    extra={
                        "webhook_event_id": webhook_event_id,
                        "event_id": event.event_id,
                        "retry_count": event.retry_count,
                        "error": error_message,
                    },
                )
            else:
                # Schedule retry with exponential backoff: 10s, 20s, 40s, 80s, 160s
                backoff_seconds = min(10 * (2 ** (event.retry_count - 1)), 300)
                event.retry_after = datetime.now(timezone.utc) + timedelta(
                    seconds=backoff_seconds
                )
                event.status = WebhookEventStatus.RETRY_PENDING
                logger.info(
                    "webhook_worker.retry.scheduled",
                    extra={
                        "webhook_event_id": webhook_event_id,
                        "event_id": event.event_id,
                        "retry_count": event.retry_count,
                        "retry_after": event.retry_after.isoformat()
                        if event.retry_after
                        else None,
                    },
                )

            await db.commit()
