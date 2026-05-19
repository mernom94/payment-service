"""
app/domain/payments/state_machine.py — Payment state machine.

A pure-Python state machine with no side effects. It only knows about
valid transitions and raises InvalidPaymentStateError for illegal ones.
All persistence is the caller's responsibility.

Having this as a separate, stateless module makes it trivially unit-testable.
"""

import logging
from typing import Optional

from app.core.constants import VALID_PAYMENT_TRANSITIONS, PaymentState
from app.core.exceptions import InvalidPaymentStateError
from app.domain.payments.models import Payment

logger = logging.getLogger(__name__)


class PaymentStateMachine:
    """
    Validates and applies state transitions to a Payment ORM object.

    Usage:
        machine = PaymentStateMachine(payment)
        machine.transition_to(PaymentState.PROCESSING)
        # payment.state is now PROCESSING — caller must flush/commit to DB.
    """

    def __init__(self, payment: Payment) -> None:
        self._payment = payment

    @property
    def current_state(self) -> PaymentState:
        return PaymentState(self._payment.state)

    def can_transition_to(self, target: PaymentState) -> bool:
        """Return True if the transition is valid without raising."""
        allowed = VALID_PAYMENT_TRANSITIONS.get(self.current_state, set())
        return target in allowed

    def transition_to(
        self,
        target: PaymentState,
        *,
        error_message: Optional[str] = None,
        bunq_payment_id: Optional[str] = None,
    ) -> None:
        """
        Apply a state transition, mutating the Payment object.

        Raises InvalidPaymentStateError if the transition is not permitted.
        The caller is responsible for committing the session.

        Args:
            target: The desired next state.
            error_message: Stored in last_error when transitioning to FAILED.
            bunq_payment_id: Stored when bunq confirms payment submission.
        """
        if not self.can_transition_to(target):
            raise InvalidPaymentStateError(
                current=self._payment.state,
                attempted=target.value,
            )

        previous = self._payment.state
        self._payment.state = target.value

        if target == PaymentState.FAILED and error_message:
            self._payment.last_error = error_message
            # NOTE: payment.retry_count is NOT incremented here.
            #
            # Retry count authority: outbox.retry_count is the single source
            # of truth for retry logic.  It is incremented exclusively in
            # PaymentWorker._handle_failure() and drives all backoff scheduling.
            #
            # payment.retry_count exists for observability only — it reflects
            # how many times this payment has been attempted, but it is never
            # read by retry or scheduling logic.  It is incremented in
            # PaymentWorker._handle_failure() alongside outbox.retry_count so
            # both columns stay in sync and callers can inspect the payment
            # record without joining to the outbox table.
            #
            # Do NOT increment payment.retry_count here in the state machine:
            # the state machine is called from multiple paths (worker failures,
            # webhook rejections) and only the worker path should bump the count.
            # Incrementing here would cause double-counting and drift between
            # outbox.retry_count and payment.retry_count.

        if target == PaymentState.SUBMITTED and bunq_payment_id:
            self._payment.bunq_payment_id = bunq_payment_id

        if target == PaymentState.CONFIRMED:
            from datetime import datetime, timezone

            self._payment.confirmed_at = datetime.now(timezone.utc)

        logger.info(
            "payment.state_transition",
            extra={
                "payment_id": str(self._payment.id),
                "from_state": previous,
                "to_state": target.value,
                "bunq_payment_id": bunq_payment_id,
                "retry_count": self._payment.retry_count,
            },
        )

    def assert_in_state(self, *allowed_states: PaymentState) -> None:
        """
        Assert the payment is in one of the allowed states.
        Useful as a guard at the start of service methods.
        """
        if self.current_state not in allowed_states:
            raise InvalidPaymentStateError(
                current=self._payment.state,
                attempted=f"one of {[s.value for s in allowed_states]}",
            )
