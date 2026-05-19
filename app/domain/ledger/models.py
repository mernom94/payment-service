"""
app/domain/ledger/models.py — Ledger ORM models.

The ledger is append-only. Rows are never UPDATEd or DELETEd after insertion.
Corrections are made via reversal entries.

Two tables:
  - LedgerAccount   — represents a financial account (mirrors a bunq monetary account).
  - LedgerEntry     — a single debit or credit line within a transaction.

Every financial event creates an even number of LedgerEntry rows that sum to zero
(double-entry accounting invariant).
"""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.constants import AccountType
from app.infrastructure.db.base import Base


class LedgerAccount(Base):
    """
    An internal account that mirrors a bunq monetary account.

    bunq_account_id links this to the real bunq account. We maintain a
    running balance here so reconciliation can detect drift without
    summing all entries each time.
    """

    __tablename__ = "ledger_accounts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    bunq_account_id: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    account_type: Mapped[str] = mapped_column(
        String(20), nullable=False, default=AccountType.PAYMENT
    )
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Denormalised running balance — updated atomically with each entry batch.
    # Derived truth is in LedgerEntry rows; this is a cache for performance.
    balance: Mapped[Decimal] = mapped_column(
        Numeric(precision=18, scale=2), nullable=False, default=Decimal("0.00")
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

    entries: Mapped[list["LedgerEntry"]] = relationship(
        "LedgerEntry", back_populates="account", lazy="select"
    )


class LedgerEntry(Base):
    """
    A single debit or credit entry in the ledger.

    IMMUTABLE after insert. Never update or delete rows in this table.

    entry_type:
      DEBIT  — money leaving the account (or a liability increasing).
      CREDIT — money entering the account (or a liability decreasing).

    The sign convention follows standard accounting:
      Assets: DEBIT increases balance, CREDIT decreases.
    For simplicity in this system, DEBIT = outflow, CREDIT = inflow.

    Every set of entries for a transaction must net to zero:
      sum(DEBIT amounts) == sum(CREDIT amounts)
    """

    __tablename__ = "ledger_entries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # Links to the internal account being affected.
    account_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("ledger_accounts.id"), nullable=False, index=True
    )

    # Links to the payment that caused this entry (nullable for manual/
    # reconciliation entries).
    payment_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("payments.id"), nullable=True, index=True
    )

    entry_type: Mapped[str] = mapped_column(
        String(10), nullable=False
    )  # DEBIT | CREDIT
    amount: Mapped[Decimal] = mapped_column(
        Numeric(precision=18, scale=2), nullable=False
    )
    currency: Mapped[str] = mapped_column(String(3), nullable=False)

    # Human-readable description for audit purposes.
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Reference ID groups all entries in the same double-entry transaction.
    transaction_ref: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # Immutable timestamp — never updated.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    account: Mapped["LedgerAccount"] = relationship(
        "LedgerAccount", back_populates="entries"
    )
