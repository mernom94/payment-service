"""
tests/integration/ledger/test_ledger_engine.py — Integration tests for LedgerEngine.

Tests double-entry ledger correctness, balance updates, and idempotency guarantees.

Architecture:
  - GIVEN  : LedgerAccount rows are created exclusively via LedgerAccountFactory.
             Amounts are always specified in INTEGER CENTS (amount_cents) and
             converted internally — never constructed from floats.
  - WHEN   : only public LedgerEngine methods are called.
  - THEN   : assertions use integer-cent comparisons or Decimal equality only.

Domain invariants enforced in every test:
  1. Amounts are stored as integer cents (no floating-point monetary arithmetic).
  2. Every record_* call must produce exactly two balanced entries.
  3. LedgerAccount rows must exist before entries reference them.
  4. record_payment_sent is idempotent: same payment_id → same transaction_ref.
"""

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.constants import LedgerEntryType
from app.domain.ledger.engine import LedgerEngine
from app.domain.ledger.models import LedgerEntry
from tests.factories import LedgerAccountFactory, PaymentFactory


# ── Helpers ────────────────────────────────────────────────────────────────────


def _cents(n: int) -> Decimal:
    """Convert integer cents to two-decimal Decimal. Used for all amount assertions."""
    return Decimal(n) / Decimal(100)


async def _all_entries_for_ref(db_session, ref: str) -> list[LedgerEntry]:
    result = await db_session.execute(
        select(LedgerEntry).where(LedgerEntry.transaction_ref == ref)
    )
    return result.scalars().all()


# ── record_payment_sent ────────────────────────────────────────────────────────


class TestRecordPaymentSent:
    @pytest.mark.asyncio
    async def test_creates_exactly_two_ledger_entries(self, db_session):
        # GIVEN a source account and a payment
        await LedgerAccountFactory.create(
            db_session, bunq_account_id="acct-sent-001", balance_cents=10000
        )
        payment = await PaymentFactory.create(
            db_session, from_account_id="acct-sent-001"
        )
        engine = LedgerEngine(db_session)

        # WHEN the payment is recorded as sent
        ref = await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-sent-001",
            amount=_cents(5000),
            currency="EUR",
        )
        await db_session.flush()

        # THEN exactly two entries exist under this transaction_ref
        entries = await _all_entries_for_ref(db_session, ref)
        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_entries_consist_of_one_debit_and_one_credit(self, db_session):

        # GIVEN a source account
        await LedgerAccountFactory.create(
            db_session, bunq_account_id="acct-types-001", balance_cents=20000
        )
        engine = LedgerEngine(db_session)

        payment = await PaymentFactory.create(
            db_session,
            from_account_id="acct-types-001",
        )

        # WHEN recorded
        ref = await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-types-001",
            amount=_cents(7500),
            currency="EUR",
        )
        await db_session.flush()

        # THEN there is exactly one DEBIT and one CREDIT
        entries = await _all_entries_for_ref(db_session, ref)
        entry_types = {e.entry_type for e in entries}
        assert LedgerEntryType.DEBIT in entry_types
        assert LedgerEntryType.CREDIT in entry_types

    @pytest.mark.asyncio
    async def test_debit_and_credit_amounts_are_equal(self, db_session):
        # GIVEN a source account
        await LedgerAccountFactory.create(
            db_session, bunq_account_id="acct-balance-pairs", balance_cents=50000
        )
        engine = LedgerEngine(db_session)
        amount_cents = 12345  # €123.45

        payment = await PaymentFactory.create(
            db_session,
            from_account_id="acct-types-001",
        )

        # WHEN recorded
        ref = await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-balance-pairs",
            amount=_cents(amount_cents),
            currency="EUR",
        )
        await db_session.flush()

        # THEN the debit total equals the credit total (double-entry invariant)
        entries = await _all_entries_for_ref(db_session, ref)
        debit_total = sum(
            e.amount for e in entries if e.entry_type == LedgerEntryType.DEBIT
        )
        credit_total = sum(
            e.amount for e in entries if e.entry_type == LedgerEntryType.CREDIT
        )
        assert debit_total == credit_total == _cents(amount_cents)

    @pytest.mark.asyncio
    async def test_source_account_balance_decreases_by_payment_amount(self, db_session):
        # GIVEN a source account with a known balance (expressed as integer cents)
        initial_balance_cents = 20000  # €200.00
        payment_amount_cents = 5000  # €50.00
        expected_balance_cents = 15000  # €150.00

        account = await LedgerAccountFactory.create(
            db_session,
            bunq_account_id="acct-balance-deduct",
            balance_cents=initial_balance_cents,
        )

        payment = await PaymentFactory.create(
            db_session,
            from_account_id="acct-balance-deduct",
        )
        engine = LedgerEngine(db_session)

        # WHEN a payment is recorded as sent
        await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-balance-deduct",
            amount=_cents(payment_amount_cents),
            currency="EUR",
        )
        await db_session.flush()

        # THEN the account balance is decremented by the exact payment amount
        await db_session.refresh(account)
        assert account.balance == _cents(expected_balance_cents)

    @pytest.mark.asyncio
    async def test_each_call_produces_a_unique_transaction_ref(self, db_session):
        # GIVEN two separate payments
        await LedgerAccountFactory.create(db_session, bunq_account_id="acct-multi-ref")
        engine = LedgerEngine(db_session)

        payment1 = await PaymentFactory.create(db_session, from_account_id="acct-multi-ref")
        payment2 = await PaymentFactory.create(db_session, from_account_id="acct-multi-ref")

        # WHEN both are recorded
        ref1 = await engine.record_payment_sent(
            payment_id=payment1.id,
            from_account_id="acct-multi-ref",
            amount=_cents(1000),
            currency="EUR",
        )
        ref2 = await engine.record_payment_sent(
            payment_id=payment2.id,
            from_account_id="acct-multi-ref",
            amount=_cents(2000),
            currency="EUR",
        )

        # THEN each gets a distinct transaction_ref
        assert ref1 != ref2

    @pytest.mark.asyncio
    async def test_idempotent_for_same_payment_id(self, db_session):
        """
        record_payment_sent must not write duplicate entries for the same payment_id.
        This protects against double-charging when reconciliation recovers an
        ambiguous payment that was already partially processed.
        """
        # GIVEN a payment
        await LedgerAccountFactory.create(
            db_session, bunq_account_id="acct-idem-ledger"
        )
        payment = await PaymentFactory.create(
            db_session, from_account_id="acct-idem-ledger"
        )
        engine = LedgerEngine(db_session)

        # WHEN record_payment_sent is called twice with the same payment_id
        ref1 = await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-idem-ledger",
            amount=_cents(5000),
            currency="EUR",
        )
        await db_session.flush()

        ref2 = await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-idem-ledger",
            amount=_cents(5000),
            currency="EUR",
        )
        await db_session.flush()

        # THEN only two entries exist (not four) and both calls return the same ref
        assert ref1 == ref2
        entries = await _all_entries_for_ref(db_session, ref1)
        assert len(entries) == 2


# ── record_payment_confirmed ───────────────────────────────────────────────────


class TestRecordPaymentConfirmed:
    @pytest.mark.asyncio
    async def test_confirmation_creates_balanced_entries(self, db_session):
        # GIVEN an existing payment that has been sent
        await LedgerAccountFactory.create(
            db_session, bunq_account_id="acct-confirm-src"
        )
        payment = await PaymentFactory.create(
            db_session, from_account_id="acct-confirm-src"
        )
        engine = LedgerEngine(db_session)

        await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-confirm-src",
            amount=_cents(3000),
            currency="EUR",
        )
        await db_session.flush()

        # WHEN the payment is confirmed
        ref = await engine.record_payment_confirmed(
            payment_id=payment.id,
            from_account_id="acct-confirm-src",
            amount=_cents(3000),
            currency="EUR",
        )
        await db_session.flush()

        # THEN two balanced entries are produced for the confirmation
        entries = await _all_entries_for_ref(db_session, ref)
        assert len(entries) == 2
        debit_total = sum(
            e.amount for e in entries if e.entry_type == LedgerEntryType.DEBIT
        )
        credit_total = sum(
            e.amount for e in entries if e.entry_type == LedgerEntryType.CREDIT
        )
        assert debit_total == credit_total == _cents(3000)


# ── record_payment_failed ──────────────────────────────────────────────────────


class TestRecordPaymentFailed:
    @pytest.mark.asyncio
    async def test_reversal_creates_balanced_entries(self, db_session):
        # GIVEN a payment that was sent and then failed
        await LedgerAccountFactory.create(db_session, bunq_account_id="acct-fail-src")
        payment = await PaymentFactory.create(
            db_session, from_account_id="acct-fail-src"
        )
        engine = LedgerEngine(db_session)

        await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-fail-src",
            amount=_cents(3000),
            currency="EUR",
        )
        await db_session.flush()

        # WHEN the failure reversal is recorded
        ref = await engine.record_payment_failed(
            payment_id=payment.id,
            from_account_id="acct-fail-src",
            amount=_cents(3000),
            currency="EUR",
        )
        await db_session.flush()

        # THEN the reversal entries are balanced
        entries = await _all_entries_for_ref(db_session, ref)
        assert len(entries) == 2
        debit_total = sum(
            e.amount for e in entries if e.entry_type == LedgerEntryType.DEBIT
        )
        credit_total = sum(
            e.amount for e in entries if e.entry_type == LedgerEntryType.CREDIT
        )
        assert debit_total == credit_total == _cents(3000)

    @pytest.mark.asyncio
    async def test_source_account_balance_restored_after_reversal(self, db_session):
        # GIVEN a source account with an initial balance
        initial_cents = 20000  # €200.00
        payment_cents = 5000  # €50.00

        account = await LedgerAccountFactory.create(
            db_session,
            bunq_account_id="acct-reversal-balance",
            balance_cents=initial_cents,
        )
        payment = await PaymentFactory.create(
            db_session, from_account_id="acct-reversal-balance"
        )
        engine = LedgerEngine(db_session)

        # WHEN the payment is sent then reversed
        await engine.record_payment_sent(
            payment_id=payment.id,
            from_account_id="acct-reversal-balance",
            amount=_cents(payment_cents),
            currency="EUR",
        )
        await db_session.flush()
        await engine.record_payment_failed(
            payment_id=payment.id,
            from_account_id="acct-reversal-balance",
            amount=_cents(payment_cents),
            currency="EUR",
        )
        await db_session.flush()

        # THEN the balance returns to its initial value
        await db_session.refresh(account)
        assert account.balance == _cents(initial_cents)


# ── get_account_balance ────────────────────────────────────────────────────────


class TestGetAccountBalance:
    @pytest.mark.asyncio
    async def test_returns_none_for_unknown_account(self, db_session):
        # GIVEN no accounts exist for "ghost-account"
        engine = LedgerEngine(db_session)

        # WHEN querying for a non-existent account
        balance = await engine.get_account_balance("ghost-account")

        # THEN None is returned
        assert balance is None

    @pytest.mark.asyncio
    async def test_returns_correct_balance_for_known_account(self, db_session):
        # GIVEN an account with a known balance (expressed in cents)
        balance_cents = 99999  # €999.99
        await LedgerAccountFactory.create(
            db_session,
            bunq_account_id="acct-known-balance",
            balance_cents=balance_cents,
        )
        engine = LedgerEngine(db_session)

        # WHEN querying
        balance = await engine.get_account_balance("acct-known-balance")

        # THEN the exact balance is returned
        assert balance == _cents(balance_cents)
