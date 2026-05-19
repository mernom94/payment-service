"""
tests/factories/ledger_factory.py — Factories for LedgerAccount and LedgerEntry.

All ledger test data must be created through these factories.

Critical domain rules enforced here:
  - Amounts are stored as INTEGER CENTS internally in tests to mirror
    the production invariant that monetary values must not use floating-point.
    The factory converts to Decimal(cents) / 100 when persisting, matching
    the ORM column type (Numeric precision=18 scale=2).
  - Every LedgerEntry must reference a valid LedgerAccount row — no orphan
    entries are permitted.
  - Factories do NOT create accounts implicitly inside business logic;
    accounts are always explicit test setup.
"""

import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import AccountType, LedgerEntryType
from app.domain.ledger.models import LedgerAccount, LedgerEntry
from app.domain.payments.models import Payment


def _cents_to_decimal(cents: int) -> Decimal:
    """
    Convert integer cents to a two-decimal-place Decimal.

    This is the canonical conversion used throughout the test suite to ensure
    monetary values are never constructed from floating-point literals.

    Example: 4250 → Decimal("42.50")
    """
    return Decimal(cents) / Decimal(100)


class LedgerAccountFactory:
    """
    Creates fully-persisted LedgerAccount rows.

    Usage:
        account = await LedgerAccountFactory.create(db_session)
        account = await LedgerAccountFactory.create(
            db_session,
            bunq_account_id="acct-xyz",
            balance_cents=20000,   # £200.00
            currency="GBP",
        )
    """

    @staticmethod
    async def create(
        db: AsyncSession,
        *,
        bunq_account_id: Optional[str] = None,
        account_type: AccountType = AccountType.PAYMENT,
        currency: str = "EUR",
        balance_cents: int = 0,
        description: Optional[str] = None,
    ) -> LedgerAccount:
        """
        Persist and return a LedgerAccount.

        balance_cents: balance expressed as INTEGER CENTS to enforce the domain
        rule that monetary values must never be floating-point. Converted to
        Decimal before persisting.
        """
        account = LedgerAccount(
            id=uuid.uuid4(),
            bunq_account_id=bunq_account_id or f"test-acct-{uuid.uuid4().hex[:10]}",
            account_type=account_type,
            currency=currency,
            balance=_cents_to_decimal(balance_cents),
            description=description,
        )
        db.add(account)
        await db.flush()
        await db.refresh(account)
        return account

    @staticmethod
    async def create_suspense(
        db: AsyncSession,
        *,
        currency: str = "EUR",
        balance_cents: int = 0,
    ) -> LedgerAccount:
        """Create the canonical suspense account for a given currency."""
        return await LedgerAccountFactory.create(
            db,
            bunq_account_id=f"SUSPENSE_{currency}",
            account_type=AccountType.SUSPENSE,
            currency=currency,
            balance_cents=balance_cents,
        )

    @staticmethod
    async def create_external(
        db: AsyncSession,
        *,
        currency: str = "EUR",
    ) -> LedgerAccount:
        """Create the canonical external settlement account for a currency."""
        return await LedgerAccountFactory.create(
            db,
            bunq_account_id=f"EXTERNAL_{currency}",
            currency=currency,
        )


class LedgerEntryFactory:
    """
    Creates fully-persisted LedgerEntry rows.

    IMPORTANT: This factory never creates unbalanced entries on its own.
    Use create_balanced_pair() to produce a valid debit/credit pair.

    Usage:
        debit, credit = await LedgerEntryFactory.create_balanced_pair(
            db_session,
            debit_account=source_account,
            credit_account=suspense_account,
            amount_cents=5000,    # €50.00
        )
    """

    @staticmethod
    async def create(
        db: AsyncSession,
        *,
        account: LedgerAccount,
        entry_type: LedgerEntryType,
        amount_cents: int,
        currency: str = "EUR",
        transaction_ref: Optional[str] = None,
        payment: Optional[Payment] = None,
        description: Optional[str] = None,
    ) -> LedgerEntry:
        """
        Persist and return a single LedgerEntry.

        amount_cents: amount in integer cents (e.g. 5000 = €50.00).
        Callers are responsible for ensuring balance across entry pairs.
        """
        entry = LedgerEntry(
            id=uuid.uuid4(),
            account_id=account.id,
            payment_id=payment.id if payment else None,
            entry_type=entry_type,
            amount=_cents_to_decimal(amount_cents),
            currency=currency,
            transaction_ref=transaction_ref or str(uuid.uuid4()),
            description=description,
        )
        db.add(entry)
        await db.flush()
        await db.refresh(entry)
        return entry

    @staticmethod
    async def create_balanced_pair(
        db: AsyncSession,
        *,
        debit_account: LedgerAccount,
        credit_account: LedgerAccount,
        amount_cents: int,
        currency: str = "EUR",
        payment: Optional[Payment] = None,
        description: Optional[str] = None,
    ) -> tuple[LedgerEntry, LedgerEntry]:
        """
        Create a balanced debit/credit pair sharing the same transaction_ref.

        This is the primary way to create valid ledger entries in tests. The
        pair always sums to zero, satisfying the double-entry invariant.

        Returns (debit_entry, credit_entry).
        """
        transaction_ref = str(uuid.uuid4())
        debit = await LedgerEntryFactory.create(
            db,
            account=debit_account,
            entry_type=LedgerEntryType.DEBIT,
            amount_cents=amount_cents,
            currency=currency,
            transaction_ref=transaction_ref,
            payment=payment,
            description=description or f"Test debit {transaction_ref[:8]}",
        )
        credit = await LedgerEntryFactory.create(
            db,
            account=credit_account,
            entry_type=LedgerEntryType.CREDIT,
            amount_cents=amount_cents,
            currency=currency,
            transaction_ref=transaction_ref,
            payment=payment,
            description=description or f"Test credit {transaction_ref[:8]}",
        )
        return debit, credit
