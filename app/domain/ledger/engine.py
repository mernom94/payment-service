"""
app/domain/ledger/engine.py — Double-entry ledger engine.

Responsible for writing balanced pairs of ledger entries. The engine
enforces the core invariant: every transaction must sum to zero before
being persisted.

All writes happen inside the caller's DB transaction — the engine never
manages its own transaction boundary. The caller commits or rolls back.

Idempotency guarantees (all three public write methods):
  - record_payment_sent:     idempotent on (payment_id, DEBIT entry)
  - record_payment_confirmed: idempotent on (payment_id, CREDIT + "confirmed")
  - record_payment_failed:   idempotent on (payment_id, CREDIT + "reversal")

Without idempotency on record_payment_failed, the reconciliation worker
could double-reverse a failed payment's suspense entry on retry, crediting
the source account twice and leaving the suspense account negative.
"""

import logging
import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import LedgerEntryType
from app.domain.ledger.invariants import assert_entries_balance
from app.domain.ledger.models import LedgerAccount, LedgerEntry

logger = logging.getLogger(__name__)


class LedgerEngine:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    # ── Public API ────────────────────────────────────────────────────────────

    async def record_payment_sent(
        self,
        *,
        payment_id: uuid.UUID,
        from_account_id: str,
        amount: Decimal,
        currency: str,
        description: Optional[str] = None,
    ) -> str:
        """
        Record that a payment has been sent from from_account_id.

        Idempotent: if a DEBIT entry for this payment_id already exists, the
        existing transaction_ref is returned and no new entries are written.
        This prevents double-charging when the reconciliation worker recovers
        an ambiguous payment that was already partially processed.

        Creates two entries:
          DEBIT  from_account  (outflow — asset decreases)
          CREDIT suspense      (liability — pending settlement)

        Returns the transaction_ref that groups the two entries.
        """
        # Idempotency check: if a DEBIT entry already exists for this payment,
        # return its transaction_ref without writing again.
        existing = await self._db.execute(
            select(LedgerEntry).where(
                LedgerEntry.payment_id == payment_id,
                LedgerEntry.entry_type == LedgerEntryType.DEBIT,
            )
        )
        existing_entry = existing.scalar_one_or_none()
        if existing_entry:
            logger.info(
                "ledger.payment_sent.already_recorded",
                extra={
                    "payment_id": str(payment_id),
                    "transaction_ref": existing_entry.transaction_ref,
                },
            )
            return existing_entry.transaction_ref

        transaction_ref = str(uuid.uuid4())

        from_account = await self._get_or_create_account(from_account_id, currency)
        suspense_account = await self._get_or_create_suspense_account(currency)

        entries = [
            LedgerEntry(
                id=uuid.uuid4(),
                account_id=from_account.id,
                payment_id=payment_id,
                entry_type=LedgerEntryType.DEBIT,
                amount=amount,
                currency=currency,
                description=description or f"Payment sent: {payment_id}",
                transaction_ref=transaction_ref,
            ),
            LedgerEntry(
                id=uuid.uuid4(),
                account_id=suspense_account.id,
                payment_id=payment_id,
                entry_type=LedgerEntryType.CREDIT,
                amount=amount,
                currency=currency,
                description=description or f"Payment suspense: {payment_id}",
                transaction_ref=transaction_ref,
            ),
        ]

        await self._write_entries(
            entries, from_account, suspense_account, amount, currency
        )

        logger.info(
            "ledger.payment_sent",
            extra={
                "payment_id": str(payment_id),
                "transaction_ref": transaction_ref,
                "amount": str(amount),
                "currency": currency,
            },
        )
        return transaction_ref

    async def record_payment_confirmed(
        self,
        *,
        payment_id: uuid.UUID,
        from_account_id: str,
        amount: Decimal,
        currency: str,
        description: Optional[str] = None,
    ) -> str:
        """
        Record that a payment has been confirmed by bunq.

        Idempotent: if a CREDIT entry to the external account already exists for
        this payment_id, the existing transaction_ref is returned and no new
        entries are written. This prevents double-booking when a webhook is
        delivered more than once.

        Clears the suspense entry created by record_payment_sent:
          DEBIT  suspense      (clears the liability)
          CREDIT external      (representing the outbound transfer settlement)

        Returns the transaction_ref.
        """
        # Idempotency check: if a CREDIT to the external account already exists
        # for this payment, return its transaction_ref without writing again.
        existing = await self._db.execute(
            select(LedgerEntry).where(
                LedgerEntry.payment_id == payment_id,
                LedgerEntry.entry_type == LedgerEntryType.CREDIT,
                LedgerEntry.description.like("%confirmed%"),
            )
        )
        existing_entry = existing.scalar_one_or_none()
        if existing_entry:
            logger.info(
                "ledger.payment_confirmed.already_recorded",
                extra={
                    "payment_id": str(payment_id),
                    "transaction_ref": existing_entry.transaction_ref,
                },
            )
            return existing_entry.transaction_ref

        transaction_ref = str(uuid.uuid4())

        suspense_account = await self._get_or_create_suspense_account(currency)
        external_account = await self._get_or_create_external_account(currency)

        entries = [
            LedgerEntry(
                id=uuid.uuid4(),
                account_id=suspense_account.id,
                payment_id=payment_id,
                entry_type=LedgerEntryType.DEBIT,
                amount=amount,
                currency=currency,
                description=description or f"Payment confirmed: {payment_id}",
                transaction_ref=transaction_ref,
            ),
            LedgerEntry(
                id=uuid.uuid4(),
                account_id=external_account.id,
                payment_id=payment_id,
                entry_type=LedgerEntryType.CREDIT,
                amount=amount,
                currency=currency,
                description=description or f"External settlement: {payment_id}",
                transaction_ref=transaction_ref,
            ),
        ]

        await self._write_entries(
            entries, suspense_account, external_account, amount, currency
        )

        logger.info(
            "ledger.payment_confirmed",
            extra={
                "payment_id": str(payment_id),
                "transaction_ref": transaction_ref,
                "amount": str(amount),
                "currency": currency,
            },
        )
        return transaction_ref

    async def record_payment_failed(
        self,
        *,
        payment_id: uuid.UUID,
        from_account_id: str,
        amount: Decimal,
        currency: str,
    ) -> str:
        """
        Reverse the suspense entry when a payment fails.

        Idempotent: if a CREDIT reversal entry already exists for this payment_id,
        the existing transaction_ref is returned and no new entries are written.

        Without this guard, the webhook processor could double-reverse a failed
        payment on retry (e.g. a REJECTED webhook delivered twice), crediting
        the source account twice and leaving the suspense account negative —
        a silent ledger corruption that reconciliation would eventually catch,
        but too late for a production system.

        Creates a reversal pair:
          DEBIT  suspense       (clears the outstanding liability)
          CREDIT from_account   (returns the funds to the source account)
        """
        # Idempotency check: if a reversal CREDIT entry already exists for this
        # payment_id, return its transaction_ref without writing again.
        existing = await self._db.execute(
            select(LedgerEntry).where(
                LedgerEntry.payment_id == payment_id,
                LedgerEntry.entry_type == LedgerEntryType.CREDIT,
                LedgerEntry.description.like("%reversal%"),
            )
        )
        existing_entry = existing.scalar_one_or_none()
        if existing_entry:
            logger.info(
                "ledger.payment_failed.already_recorded",
                extra={
                    "payment_id": str(payment_id),
                    "transaction_ref": existing_entry.transaction_ref,
                },
            )
            return existing_entry.transaction_ref

        transaction_ref = str(uuid.uuid4())

        from_account = await self._get_or_create_account(from_account_id, currency)
        suspense_account = await self._get_or_create_suspense_account(currency)

        entries = [
            LedgerEntry(
                id=uuid.uuid4(),
                account_id=suspense_account.id,
                payment_id=payment_id,
                entry_type=LedgerEntryType.DEBIT,
                amount=amount,
                currency=currency,
                description=f"Payment reversal (failed): {payment_id}",
                transaction_ref=transaction_ref,
            ),
            LedgerEntry(
                id=uuid.uuid4(),
                account_id=from_account.id,
                payment_id=payment_id,
                entry_type=LedgerEntryType.CREDIT,
                amount=amount,
                currency=currency,
                description=f"Funds returned (failed payment): {payment_id}",
                transaction_ref=transaction_ref,
            ),
        ]

        await self._write_entries(
            entries, suspense_account, from_account, amount, currency
        )

        logger.info(
            "ledger.payment_failed",
            extra={
                "payment_id": str(payment_id),
                "transaction_ref": transaction_ref,
                "amount": str(amount),
                "currency": currency,
            },
        )
        return transaction_ref

    async def get_account_balance(self, bunq_account_id: str) -> Optional[Decimal]:
        """Return the current ledger balance for a bunq account ID."""
        result = await self._db.execute(
            select(LedgerAccount.balance).where(
                LedgerAccount.bunq_account_id == bunq_account_id
            )
        )
        return result.scalar_one_or_none()

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _write_entries(
        self,
        entries: list[LedgerEntry],
        debit_account: LedgerAccount,
        credit_account: LedgerAccount,
        amount: Decimal,
        currency: str,
    ) -> None:
        """
        Assert balance, insert entries, update running balances — all atomically.

        We SELECT FOR UPDATE both account rows before updating balances. This
        serialises concurrent balance updates on the same account at the row
        level rather than relying on SERIALIZABLE transaction retries, which
        would cause legitimate payments to fail permanently under high concurrency
        to the same account.

        The assert_entries_balance call runs BEFORE we touch the DB. If it
        raises, nothing is written.
        """
        assert_entries_balance(entries)

        # Lock both account rows to serialise balance updates.
        # Order by id to prevent deadlock when two transactions update the
        # same pair of accounts in opposite order.
        account_ids = sorted([debit_account.id, credit_account.id])
        await self._db.execute(
            select(LedgerAccount)
            .where(LedgerAccount.id.in_(account_ids))
            .with_for_update()
        )

        for entry in entries:
            self._db.add(entry)

        # Update the denormalised running balances.
        await self._db.execute(
            update(LedgerAccount)
            .where(LedgerAccount.id == debit_account.id)
            .values(balance=LedgerAccount.balance - amount)
        )
        await self._db.execute(
            update(LedgerAccount)
            .where(LedgerAccount.id == credit_account.id)
            .values(balance=LedgerAccount.balance + amount)
        )

        await self._db.flush()

    async def _get_or_create_account(
        self, bunq_account_id: str, currency: str
    ) -> LedgerAccount:
        """
        Return the ledger account for bunq_account_id, creating it if needed.

        Uses INSERT ... ON CONFLICT DO NOTHING to handle the race where two
        concurrent workers both find None and try to insert simultaneously.
        The second writer's INSERT is silently ignored and then we re-fetch
        the row inserted by the first writer.
        """
        result = await self._db.execute(
            select(LedgerAccount).where(
                LedgerAccount.bunq_account_id == bunq_account_id
            )
        )
        account = result.scalar_one_or_none()
        if account:
            return account

        # Attempt insert with ON CONFLICT DO NOTHING.
        new_id = uuid.uuid4()
        stmt = (
            pg_insert(LedgerAccount)
            .values(
                id=new_id,
                bunq_account_id=bunq_account_id,
                currency=currency,
            )
            .on_conflict_do_nothing(index_elements=["bunq_account_id"])
        )
        await self._db.execute(stmt)
        await self._db.flush()

        # Re-fetch: either the row we just inserted or the one from the
        # concurrent winner.
        result = await self._db.execute(
            select(LedgerAccount).where(
                LedgerAccount.bunq_account_id == bunq_account_id
            )
        )
        return result.scalar_one()

    async def _get_or_create_suspense_account(self, currency: str) -> LedgerAccount:
        return await self._get_or_create_account(
            bunq_account_id=f"SUSPENSE_{currency}", currency=currency
        )

    async def _get_or_create_external_account(self, currency: str) -> LedgerAccount:
        return await self._get_or_create_account(
            bunq_account_id=f"EXTERNAL_{currency}", currency=currency
        )
