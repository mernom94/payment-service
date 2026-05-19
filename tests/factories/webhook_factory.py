"""
tests/factories/webhook_factory.py — Factories for WebhookEvent and ProcessedWebhookEvent.

All webhook test data must be created through these factories.
Tests must never instantiate WebhookEvent or ProcessedWebhookEvent directly.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import WebhookEventStatus
from app.domain.webhooks.models import ProcessedWebhookEvent, WebhookEvent


class WebhookEventFactory:
    """
    Creates fully-persisted WebhookEvent rows.

    Usage:
        event = await WebhookEventFactory.create(db_session)
        event = await WebhookEventFactory.create(
            db_session,
            event_id="evt-bunq-abc123",
            status=WebhookEventStatus.QUEUED,
        )
    """

    @staticmethod
    def _default_payload(event_type: str = "PAYMENT") -> dict[str, Any]:
        return {
            "NotificationType": event_type,
            "EventType": f"{event_type}_CREATED",
            "Payment": {
                "id": 99001,
                "status": "ACCEPTED",
                "amount": {"value": "42.50", "currency": "EUR"},
                "description": "Test payment",
                "monetary_account_id": 123456,
                "created": "2024-01-15 10:00:00.000000",
                "updated": "2024-01-15 10:00:01.000000",
            },
        }

    @staticmethod
    async def create(
        db: AsyncSession,
        *,
        event_id: Optional[str] = None,
        event_type: str = "PAYMENT",
        payload: Optional[dict[str, Any]] = None,
        raw_body: str = "{}",
        status: WebhookEventStatus = WebhookEventStatus.RECEIVED,
        error_message: Optional[str] = None,
    ) -> WebhookEvent:
        """
        Persist and return a WebhookEvent.

        A unique event_id is auto-generated if not supplied to avoid
        uniqueness collisions between tests.
        """
        event = WebhookEvent(
            id=uuid.uuid4(),
            event_id=event_id or f"evt-{uuid.uuid4().hex[:16]}",
            event_type=event_type,
            payload=payload or WebhookEventFactory._default_payload(event_type),
            raw_body=raw_body,
            status=status,
            error_message=error_message,
        )
        db.add(event)
        await db.flush()
        await db.refresh(event)
        return event

    @staticmethod
    async def create_queued(db: AsyncSession, **kwargs) -> WebhookEvent:
        """Convenience builder: a WebhookEvent in QUEUED state."""
        return await WebhookEventFactory.create(
            db,
            status=WebhookEventStatus.QUEUED,
            **kwargs,
        )

    @staticmethod
    async def create_for_payment(
        db: AsyncSession,
        *,
        bunq_payment_id: int = 99001,
        bunq_status: str = "ACCEPTED",
        amount_value: str = "42.50",
        currency: str = "EUR",
        monetary_account_id: int = 123456,
        **kwargs,
    ) -> WebhookEvent:
        """
        Convenience builder: a PAYMENT-type webhook for a specific bunq payment.

        bunq_payment_id must match the bunq_payment_id stored on the Payment row
        for state-machine dispatch to work in integration tests.
        """
        payload = {
            "NotificationType": "PAYMENT",
            "Payment": {
                "id": bunq_payment_id,
                "status": bunq_status,
                "amount": {"value": amount_value, "currency": currency},
                "monetary_account_id": monetary_account_id,
            },
        }
        return await WebhookEventFactory.create(
            db,
            event_type="PAYMENT",
            payload=payload,
            status=WebhookEventStatus.QUEUED,
            **kwargs,
        )


class ProcessedWebhookEventFactory:
    """
    Creates fully-persisted ProcessedWebhookEvent rows (deduplication log).

    These represent events that have already been processed. Used to set up
    duplicate-rejection scenarios without running the full processing pipeline.

    Usage:
        await ProcessedWebhookEventFactory.create(db_session, event_id="evt-seen")
    """

    @staticmethod
    async def create(
        db: AsyncSession,
        *,
        event_id: Optional[str] = None,
        webhook_event: Optional[WebhookEvent] = None,
        processed_at: Optional[datetime] = None,
    ) -> ProcessedWebhookEvent:
        """
        Persist and return a ProcessedWebhookEvent deduplication record.

        If webhook_event is supplied, the webhook_event_id FK is populated.
        """
        record = ProcessedWebhookEvent(
            event_id=event_id or f"evt-processed-{uuid.uuid4().hex[:12]}",
            webhook_event_id=webhook_event.id if webhook_event else None,
            processed_at=processed_at or datetime.now(timezone.utc),
        )
        db.add(record)
        await db.flush()
        await db.refresh(record)
        return record
