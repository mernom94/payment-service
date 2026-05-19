"""
app/domain/webhooks/models.py — Webhook event ORM model.

Two tables:
  - WebhookEvent       — raw event storage (append-only, never modified).
  - ProcessedWebhook   — deduplication log (event_id → processed_at).

Separating storage from deduplication means we always have the raw payload
for debugging, even for events we chose to skip.

WebhookEvent lifecycle:
  RECEIVED → QUEUED → PROCESSED (happy path)
  RECEIVED → QUEUED → FAILED → RETRY_PENDING → QUEUED → … (retry path)
  RECEIVED → QUEUED → FAILED (exhausted retries — dead letter)
  RECEIVED → QUEUED → SKIPPED (dedup hit)
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import CheckConstraint, DateTime, String, Text, func, Column
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import WebhookEventStatus
from app.infrastructure.db.base import Base


class WebhookEvent(Base):
    """
    Raw webhook event exactly as received from bunq.

    Append-only payload. The status field tracks processing lifecycle.
    retry_count and retry_after drive the retry/backoff schedule.
    """

    __tablename__ = "webhook_events"

    __table_args__ = (
        CheckConstraint(
            "status IN ('RECEIVED', 'QUEUED', 'PROCESSED', 'FAILED', 'SKIPPED', 'RETRY_PENDING')",
            name="ck_webhook_events_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    # bunq-supplied event identifier used for deduplication.
    event_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    event_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Complete raw payload for debugging and replay.
    payload = Column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )
    raw_body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=WebhookEventStatus.RECEIVED,
        index=True,
    )
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Retry tracking.
    retry_count: Mapped[int] = mapped_column(default=0, nullable=False)
    retry_after: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class ProcessedWebhookEvent(Base):
    """
    Deduplication log. One row per successfully processed event_id.

    Using a separate table (rather than a flag on WebhookEvent) means the
    deduplication check is a fast primary-key lookup, and we can index
    event_id without table bloat from the large payload column.
    """

    __tablename__ = "processed_webhook_events"

    event_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    webhook_event_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
