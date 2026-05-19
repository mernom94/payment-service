"""
app/domain/payments/models.py — Payment ORM model and Pydantic schemas.

Separates three concerns:
  - SQLAlchemy ORM model (Payment) — the DB row.
  - Pydantic request schemas (CreatePaymentRequest) — API input validation.
  - Pydantic response schemas (PaymentResponse) — API output shape.

Keeping them separate means the DB schema can evolve independently of the
API contract, and we never accidentally expose internal DB fields to callers.
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import CheckConstraint, DateTime, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.constants import PaymentState
from app.infrastructure.db.base import Base


# ── ORM Model ─────────────────────────────────────────────────────────────────


class Payment(Base):
    """
    Persisted payment record.

    The external_id column has a UNIQUE constraint — this is the second
    line of defence against duplicate payments (the first is the Redis lock).
    bunq_payment_id is populated once the payment worker successfully submits
    the payment to bunq.
    """

    __tablename__ = "payments"

    # DB-level guard: prevents ORM bugs or direct writes from inserting
    # arbitrary state strings.  The migration also adds this constraint;
    # declaring it here keeps the ORM model self-documenting and ensures
    # metadata.create_all() (used in tests) enforces the same rule.
    __table_args__ = (
        CheckConstraint(
            "state IN ('PENDING', 'PROCESSING', 'SUBMITTED', 'CONFIRMED', 'FAILED')",
            name="ck_payments_state",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    external_id: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    from_account_id: Mapped[str] = mapped_column(String(255), nullable=False)
    to_iban: Mapped[str] = mapped_column(String(34), nullable=False)
    amount: Mapped[Decimal] = mapped_column(
        Numeric(precision=18, scale=2), nullable=False
    )
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    state: Mapped[str] = mapped_column(
        String(20), nullable=False, default=PaymentState.PENDING, index=True
    )
    bunq_payment_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    retry_count: Mapped[int] = mapped_column(default=0, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    correlation_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    confirmed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


# ── Pydantic Request Schemas ──────────────────────────────────────────────────


class CreatePaymentRequest(BaseModel):
    """
    Input schema for POST /payments.

    external_id is the caller-supplied idempotency key. If the same
    external_id is submitted twice we return the first result.
    """

    external_id: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Caller-supplied idempotency key. Must be unique per payment.",
        examples=["order-9f3a2c-payment-1"],
    )
    from_account_id: str = Field(
        ...,
        description="Internal bunq monetary account ID to debit.",
        examples=["123456"],
    )
    to_iban: str = Field(
        ...,
        description="Beneficiary IBAN.",
        examples=["NL02ABNA0123456789"],
    )
    amount: Decimal = Field(
        ...,
        gt=Decimal("0"),
        description="Payment amount (positive, max 2 decimal places).",
        examples=[Decimal("42.50")],
    )
    currency: str = Field(
        ...,
        description="ISO 4217 currency code.",
        examples=["EUR"],
    )
    description: Optional[str] = Field(
        default=None,
        max_length=140,
        description="Free-text payment description.",
    )

    @field_validator("currency")
    @classmethod
    def upper_currency(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("to_iban")
    @classmethod
    def strip_iban(cls, v: str) -> str:
        return v.strip().replace(" ", "").upper()

    @field_validator("amount")
    @classmethod
    def check_decimal_places(cls, v: Decimal) -> Decimal:
        if v != v.quantize(Decimal("0.01")):
            raise ValueError("Amount must have at most 2 decimal places.")
        return v


# ── Pydantic Response Schemas ─────────────────────────────────────────────────


class PaymentResponse(BaseModel):
    """Public representation of a payment returned by the API."""

    id: uuid.UUID
    external_id: str
    from_account_id: str
    to_iban: str
    amount: Decimal
    currency: str
    description: Optional[str]
    state: PaymentState
    bunq_payment_id: Optional[str]
    retry_count: int
    correlation_id: Optional[str]
    created_at: datetime
    updated_at: datetime
    confirmed_at: Optional[datetime]

    model_config = {"from_attributes": True}


class PaymentListResponse(BaseModel):
    """Paginated list of payments."""

    items: list[PaymentResponse]
    total: int
    page: int
    page_size: int
