"""
tests/unit/test_state_machine.py — Unit tests for the payment state machine.

Every valid and invalid transition is tested explicitly. State machine bugs
are particularly dangerous in financial systems because they can leave
payments in inconsistent states.
"""

import uuid
from decimal import Decimal

import pytest

from app.core.constants import PaymentState
from app.core.exceptions import InvalidPaymentStateError
from app.domain.payments.models import Payment
from app.domain.payments.state_machine import PaymentStateMachine


def make_payment(state: PaymentState) -> Payment:
    p = Payment()
    p.id = uuid.uuid4()
    p.external_id = "test-001"
    p.from_account_id = "123"
    p.to_iban = "NL02ABNA0123456789"
    p.amount = Decimal("10.00")
    p.currency = "EUR"
    p.state = state
    p.retry_count = 0
    return p


class TestValidTransitions:
    def test_pending_to_processing(self):
        p = make_payment(PaymentState.PENDING)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.PROCESSING)
        assert p.state == PaymentState.PROCESSING

    def test_processing_to_submitted(self):
        p = make_payment(PaymentState.PROCESSING)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.SUBMITTED, bunq_payment_id="bunq-99")
        assert p.state == PaymentState.SUBMITTED
        assert p.bunq_payment_id == "bunq-99"

    def test_processing_to_failed(self):
        p = make_payment(PaymentState.PROCESSING)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.FAILED, error_message="API error")
        assert p.state == PaymentState.FAILED
        assert p.last_error == "API error"
        # retry_count is NOT incremented by the state machine. It is
        # incremented exclusively by PaymentWorker._handle_failure() so that
        # webhook-driven FAILED transitions don't inflate the counter.
        # outbox.retry_count is authoritative for backoff scheduling;
        # payment.retry_count is an observability-only mirror.
        assert p.retry_count == 0

    def test_submitted_to_confirmed(self):
        p = make_payment(PaymentState.SUBMITTED)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.CONFIRMED)
        assert p.state == PaymentState.CONFIRMED
        assert p.confirmed_at is not None

    def test_submitted_to_failed(self):
        p = make_payment(PaymentState.SUBMITTED)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.FAILED, error_message="Rejected")
        assert p.state == PaymentState.FAILED

    def test_failed_to_pending_retry(self):
        p = make_payment(PaymentState.FAILED)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.PENDING)
        assert p.state == PaymentState.PENDING

    def test_retry_count_not_incremented_by_state_machine(self):
        """
        The state machine must NOT increment payment.retry_count.

        retry_count authority: outbox.retry_count (incremented by
        PaymentWorker._handle_failure) is the single source of truth for
        backoff scheduling.  payment.retry_count is an observability-only
        mirror, also incremented by _handle_failure, never by the state
        machine.  This ensures webhook-driven FAILED transitions (e.g. bunq
        rejecting a payment via webhook) do not consume a retry slot.
        """
        p = make_payment(PaymentState.PROCESSING)
        m = PaymentStateMachine(p)
        m.transition_to(PaymentState.FAILED, error_message="err1")
        assert p.retry_count == 0  # state machine does not touch this
        m.transition_to(PaymentState.PENDING)
        m.transition_to(PaymentState.PROCESSING)
        m.transition_to(PaymentState.FAILED, error_message="err2")
        assert p.retry_count == 0  # still 0 — worker increments, not the SM


class TestInvalidTransitions:
    @pytest.mark.parametrize(
        "from_state, to_state",
        [
            (PaymentState.PENDING, PaymentState.SUBMITTED),
            (PaymentState.PENDING, PaymentState.CONFIRMED),
            (PaymentState.PENDING, PaymentState.FAILED),
            (PaymentState.PROCESSING, PaymentState.CONFIRMED),
            (PaymentState.PROCESSING, PaymentState.PENDING),
            (PaymentState.SUBMITTED, PaymentState.PROCESSING),
            (PaymentState.SUBMITTED, PaymentState.PENDING),
            (PaymentState.CONFIRMED, PaymentState.PENDING),
            (PaymentState.CONFIRMED, PaymentState.PROCESSING),
            (PaymentState.CONFIRMED, PaymentState.SUBMITTED),
            (PaymentState.CONFIRMED, PaymentState.FAILED),
        ],
    )
    def test_invalid_transition_raises(self, from_state, to_state):
        p = make_payment(from_state)
        m = PaymentStateMachine(p)
        with pytest.raises(InvalidPaymentStateError) as exc_info:
            m.transition_to(to_state)
        assert from_state.value in str(exc_info.value)
        assert to_state.value in str(exc_info.value)
        # State must not have changed.
        assert p.state == from_state

    def test_confirmed_is_terminal(self):
        """CONFIRMED is a terminal state — no transitions out."""
        p = make_payment(PaymentState.CONFIRMED)
        m = PaymentStateMachine(p)
        for target in PaymentState:
            assert not m.can_transition_to(target)


class TestCanTransitionTo:
    def test_can_transition_returns_true_for_valid(self):
        p = make_payment(PaymentState.PENDING)
        m = PaymentStateMachine(p)
        assert m.can_transition_to(PaymentState.PROCESSING) is True

    def test_can_transition_returns_false_for_invalid(self):
        p = make_payment(PaymentState.PENDING)
        m = PaymentStateMachine(p)
        assert m.can_transition_to(PaymentState.CONFIRMED) is False


class TestAssertInState:
    def test_assert_passes_when_in_state(self):
        p = make_payment(PaymentState.PENDING)
        m = PaymentStateMachine(p)
        m.assert_in_state(PaymentState.PENDING)  # No exception.

    def test_assert_passes_when_in_one_of_many(self):
        p = make_payment(PaymentState.SUBMITTED)
        m = PaymentStateMachine(p)
        m.assert_in_state(
            PaymentState.PENDING, PaymentState.SUBMITTED, PaymentState.CONFIRMED
        )

    def test_assert_raises_when_not_in_state(self):
        p = make_payment(PaymentState.CONFIRMED)
        m = PaymentStateMachine(p)
        with pytest.raises(InvalidPaymentStateError):
            m.assert_in_state(PaymentState.PENDING, PaymentState.PROCESSING)
