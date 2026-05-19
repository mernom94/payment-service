"""
app/infrastructure/db/outbox.py — Transactional outbox table.

The outbox pattern solves the dual-write problem: we cannot atomically
write to the DB AND call bunq in the same operation. Instead:

  1. Write Payment + Outbox record in one DB transaction.
  2. Worker polls the outbox, calls bunq, marks the record done.

If the worker crashes between calling bunq and marking done, it retries —
bunq will either process a duplicate (which we detect via bunq_payment_id
lookup) or return an error. Either way, the outbox record stays until we
know for certain what happened.
"""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
    Column,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import OutboxStatus
from app.infrastructure.db.base import Base


class Outbox(Base):
    """
    Outbox record representing a pending bunq API call.

    One record per payment. The worker:
      1. Selects rows WHERE status=PENDING FOR UPDATE SKIP LOCKED
         (prevents multiple workers racing on the same record).
      2. Updates status to PROCESSING.
      3. Calls bunq.
      4. Updates status to DONE (success) or FAILED (exhausted retries).
    """

    __tablename__ = "outbox"

    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED')",
            name="ck_outbox_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    payment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("payments.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
        index=True,
    )

    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=OutboxStatus.PENDING, index=True
    )

    # Full payload needed by the worker (denormalised for worker independence).
    payload = Column(
        JSON().with_variant(JSONB, "postgresql"),
        nullable=False,
    )

    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # When the worker should next attempt this record (used for backoff).
    next_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
