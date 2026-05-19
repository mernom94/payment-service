"""
tests/unit/test_ledger_invariants.py — Unit tests for ledger invariants.

Tests the pure assertion functions in app/domain/ledger/invariants.py.
No DB, no I/O. These are the most critical tests in the system — a bug
here could allow the books to become unbalanced.
"""

import uuid
from decimal import Decimal

import pytest

from app.core.constants import LedgerEntryType
from app.core.exceptions import LedgerImbalanceError
from app.domain.ledger.invariants import (
    assert_entries_balance,
    assert_matching_currencies,
    assert_non_negative_amount,
)
from app.domain.ledger.models import LedgerEntry


def make_entry(
    entry_type: str,
    amount: Decimal,
    currency: str = "EUR",
    account_id: uuid.UUID | None = None,
) -> LedgerEntry:
    entry = LedgerEntry()
    entry.id = uuid.uuid4()
    entry.account_id = account_id or uuid.uuid4()
    entry.entry_type = entry_type
    entry.amount = amount
    entry.currency = currency
    entry.transaction_ref = str(uuid.uuid4())
    return entry


class TestAssertEntriesBalance:
    def test_balanced_pair_passes(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("100.00")),
            make_entry(LedgerEntryType.CREDIT, Decimal("100.00")),
        ]
        assert_entries_balance(entries)  # Must not raise.

    def test_balanced_multi_entry_passes(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("60.00")),
            make_entry(LedgerEntryType.DEBIT, Decimal("40.00")),
            make_entry(LedgerEntryType.CREDIT, Decimal("100.00")),
        ]
        assert_entries_balance(entries)

    def test_unbalanced_raises(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("100.00")),
            make_entry(LedgerEntryType.CREDIT, Decimal("99.99")),
        ]
        with pytest.raises(LedgerImbalanceError) as exc_info:
            assert_entries_balance(entries)
        assert "0.01" in exc_info.value.imbalance

    def test_debit_only_raises(self):
        entries = [make_entry(LedgerEntryType.DEBIT, Decimal("50.00"))]
        with pytest.raises(LedgerImbalanceError):
            assert_entries_balance(entries)

    def test_credit_only_raises(self):
        entries = [make_entry(LedgerEntryType.CREDIT, Decimal("50.00"))]
        with pytest.raises(LedgerImbalanceError):
            assert_entries_balance(entries)

    def test_empty_list_raises(self):
        with pytest.raises(LedgerImbalanceError):
            assert_entries_balance([])

    def test_multi_currency_balanced_separately(self):
        """Each currency must balance independently."""
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("100.00"), "EUR"),
            make_entry(LedgerEntryType.CREDIT, Decimal("100.00"), "EUR"),
            make_entry(LedgerEntryType.DEBIT, Decimal("50.00"), "USD"),
            make_entry(LedgerEntryType.CREDIT, Decimal("50.00"), "USD"),
        ]
        assert_entries_balance(entries)

    def test_multi_currency_unbalanced_raises(self):
        """USD balances but EUR does not — should raise."""
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("100.00"), "EUR"),
            make_entry(LedgerEntryType.CREDIT, Decimal("99.00"), "EUR"),
            make_entry(LedgerEntryType.DEBIT, Decimal("50.00"), "USD"),
            make_entry(LedgerEntryType.CREDIT, Decimal("50.00"), "USD"),
        ]
        with pytest.raises(LedgerImbalanceError):
            assert_entries_balance(entries)

    def test_large_amounts_balance(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("99999.99")),
            make_entry(LedgerEntryType.CREDIT, Decimal("99999.99")),
        ]
        assert_entries_balance(entries)

    def test_zero_amount_pair_balances(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("0.00")),
            make_entry(LedgerEntryType.CREDIT, Decimal("0.00")),
        ]
        assert_entries_balance(entries)


class TestAssertNonNegativeAmount:
    def test_positive_amount_passes(self):
        assert_non_negative_amount(Decimal("1.00"))

    def test_large_positive_amount_passes(self):
        assert_non_negative_amount(Decimal("100000.00"))

    def test_zero_raises(self):
        with pytest.raises(LedgerImbalanceError):
            assert_non_negative_amount(Decimal("0.00"))

    def test_negative_raises(self):
        with pytest.raises(LedgerImbalanceError):
            assert_non_negative_amount(Decimal("-1.00"))


class TestAssertMatchingCurrencies:
    def test_same_currency_passes(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("10.00"), "EUR"),
            make_entry(LedgerEntryType.CREDIT, Decimal("10.00"), "EUR"),
        ]
        assert_matching_currencies(entries)

    def test_mixed_currencies_raises(self):
        entries = [
            make_entry(LedgerEntryType.DEBIT, Decimal("10.00"), "EUR"),
            make_entry(LedgerEntryType.CREDIT, Decimal("10.00"), "USD"),
        ]
        with pytest.raises(LedgerImbalanceError):
            assert_matching_currencies(entries)
