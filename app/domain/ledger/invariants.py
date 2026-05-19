"""
app/domain/ledger/invariants.py — Ledger invariant assertions.

Pure functions with no side effects. These are called by the ledger engine
before writing any entries to the database. If an assertion fails, the engine
aborts the write and raises LedgerImbalanceError.

Having the invariant logic in a separate module makes it easy to unit test
independently of the DB.
"""

from decimal import Decimal

from app.core.constants import LedgerEntryType
from app.core.exceptions import LedgerImbalanceError
from app.domain.ledger.models import LedgerEntry


def assert_entries_balance(entries: list[LedgerEntry]) -> None:
    """
    Assert that a list of ledger entries sums to zero (double-entry invariant).

    For each currency present in the entry list:
      sum(DEBIT amounts) must equal sum(CREDIT amounts)

    Raises LedgerImbalanceError if the invariant is violated.
    This must be called BEFORE any entries are persisted.
    """
    if not entries:
        raise LedgerImbalanceError("Empty entry list — nothing to balance.")

    # Group by currency.
    totals: dict[str, dict[str, Decimal]] = {}

    for entry in entries:
        currency = entry.currency
        if currency not in totals:
            totals[currency] = {
                LedgerEntryType.DEBIT: Decimal("0.00"),
                LedgerEntryType.CREDIT: Decimal("0.00"),
            }
        totals[currency][entry.entry_type] += entry.amount

    for currency, sums in totals.items():
        debit_total = sums[LedgerEntryType.DEBIT]
        credit_total = sums[LedgerEntryType.CREDIT]
        net = debit_total - credit_total

        if net != Decimal("0.00"):
            raise LedgerImbalanceError(
                imbalance=f"{net} {currency} "
                f"(debit={debit_total}, credit={credit_total})"
            )


def assert_non_negative_amount(amount: Decimal, label: str = "amount") -> None:
    """Assert that a monetary amount is strictly positive."""
    if amount <= Decimal("0"):
        raise LedgerImbalanceError(
            imbalance=f"{label}={amount} — ledger entries must have positive amounts."
        )


def assert_matching_currencies(entries: list[LedgerEntry]) -> None:
    """
    For a two-entry (debit/credit) pair, assert they share the same currency.
    Mixing currencies in a single entry pair is not supported.
    """
    currencies = {e.currency for e in entries}
    if len(currencies) > 1:
        raise LedgerImbalanceError(
            imbalance=f"Mixed currencies in a single entry pair: {currencies}. "
            "Cross-currency entries require explicit FX handling."
        )
