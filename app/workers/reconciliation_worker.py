"""
app/workers/reconciliation_worker.py — Reconciliation job.

Runs on a configurable interval (default: every 5 minutes). For each
known bunq monetary account:

  1. Fetch current balance from bunq.
  2. Compare with internal ledger balance.
  3. If drift detected: log a critical alert and record the error — but
     continue checking remaining accounts (previously a raise here aborted
     the entire reconciliation pass, preventing ambiguous payment recovery).

The job also handles the "ambiguous payment" recovery path: any payment stuck
in FAILED state with an "AMBIGUOUS" error is checked against bunq to see
whether it actually succeeded.

Fix for review concern: _reconcile_balances no longer raises on first drift.
It records all drifting accounts, then raises a combined error at the end if
any drift was detected.  This ensures _recover_ambiguous_payments always runs
regardless of balance state.
"""

import logging
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.constants import PaymentState
from app.core.exceptions import ReconciliationError
from app.core.observability import get_tracer, ledger_balance_drift
from app.domain.ledger.engine import LedgerEngine
from app.domain.ledger.models import LedgerAccount
from app.domain.payments.models import Payment
from app.domain.payments.state_machine import PaymentStateMachine
from app.infrastructure.bunq.client import BunqClient
from app.infrastructure.bunq.payments import BunqPaymentAdapter
from app.infrastructure.bunq.session_manager import get_session_manager
from app.infrastructure.db.session import get_session_factory
from app.infrastructure.messaging.workers import BaseWorker

_tracer = get_tracer(__name__)

logger = logging.getLogger(__name__)

TOLERANCE = Decimal("0.01")  # Allow 1 cent tolerance for rounding.


class ReconciliationWorker(BaseWorker):
    name = "reconciliation_worker"
    poll_interval = get_settings().RECONCILIATION_INTERVAL

    async def tick(self) -> None:
        """Run one reconciliation pass."""
        logger.info("reconciliation.started")

        session_manager = get_session_manager()
        async with BunqClient(session_manager) as client:
            adapter = BunqPaymentAdapter(client)
            session_factory = get_session_factory()

            async with session_factory() as db:
                # Run balance reconciliation and ambiguous payment recovery
                # independently.  A drift on one account must not prevent
                # recovery of unrelated ambiguous payments.
                drift_errors = await self._reconcile_balances(db, adapter)
                await self._recover_ambiguous_payments(db, adapter)
                await db.commit()

        if drift_errors:
            # Raise after the commit so recovery changes are persisted even
            # when drift is detected.  The exception surfaces to monitoring.
            raise ReconciliationError(
                account_id=", ".join(e.account_id for e in drift_errors),
                internal=", ".join(e.internal for e in drift_errors),
                external=", ".join(e.external for e in drift_errors),
            )

        logger.info("reconciliation.completed")

    # ── Balance reconciliation ────────────────────────────────────────────────

    async def _reconcile_balances(
        self, db: AsyncSession, adapter: BunqPaymentAdapter
    ) -> list[ReconciliationError]:
        """
        Compare internal ledger balances against live bunq balances.

        Logs a critical alert for every account with drift, but continues
        checking all accounts rather than aborting on the first drift.
        Returns a list of ReconciliationError objects (one per drifting
        account) so the caller can raise a combined error after recovery.
        """
        result = await db.execute(select(LedgerAccount))
        accounts = result.scalars().all()

        drift_errors: list[ReconciliationError] = []

        for account in accounts:
            if account.bunq_account_id.startswith(("SUSPENSE_", "EXTERNAL_")):
                continue

            bunq_balance = await adapter.get_account_balance(account.bunq_account_id)
            if bunq_balance is None:
                logger.warning(
                    "reconciliation.account_not_found",
                    extra={"bunq_account_id": account.bunq_account_id},
                )
                continue

            internal_balance = account.balance
            drift = abs(bunq_balance - internal_balance)

            if drift > TOLERANCE:
                logger.critical(
                    "reconciliation.drift_detected",
                    extra={
                        "bunq_account_id": account.bunq_account_id,
                        "internal_balance": str(internal_balance),
                        "bunq_balance": str(bunq_balance),
                        "drift": str(drift),
                    },
                )
                ledger_balance_drift.labels(account_id=account.bunq_account_id).set(
                    float(drift)
                )
                drift_errors.append(
                    ReconciliationError(
                        account_id=account.bunq_account_id,
                        internal=str(internal_balance),
                        external=str(bunq_balance),
                    )
                )
            else:
                logger.info(
                    "reconciliation.account_ok",
                    extra={
                        "bunq_account_id": account.bunq_account_id,
                        "balance": str(internal_balance),
                    },
                )

        return drift_errors

    # ── Ambiguous payment recovery ────────────────────────────────────────────

    async def _recover_ambiguous_payments(
        self, db: AsyncSession, adapter: BunqPaymentAdapter
    ) -> None:
        """
        For any payment in FAILED state with an AMBIGUOUS error, check bunq
        to see whether the payment actually succeeded.

        If it did: transition to SUBMITTED and write the ledger entry.
          record_payment_sent() is idempotent on payment_id, so calling it
          here is safe even if the original worker call partially executed.
        If it didn't: leave as FAILED and clear the ambiguous flag.
        """
        result = await db.execute(
            select(Payment).where(
                Payment.state == PaymentState.FAILED,
                Payment.last_error.like("AMBIGUOUS:%"),
            )
        )
        ambiguous_payments = result.scalars().all()

        if not ambiguous_payments:
            return

        logger.info(
            "reconciliation.checking_ambiguous_payments",
            extra={"count": len(ambiguous_payments)},
        )

        ledger = LedgerEngine(db)

        for payment in ambiguous_payments:
            if not payment.bunq_payment_id:
                logger.warning(
                    "reconciliation.ambiguous_no_bunq_id",
                    extra={"payment_id": str(payment.id)},
                )
                continue

            try:
                bunq_payment = await adapter.get_payment(
                    monetary_account_id=payment.from_account_id,
                    bunq_payment_id=payment.bunq_payment_id,
                )
            except Exception as exc:
                logger.error(
                    "reconciliation.ambiguous_lookup_failed",
                    extra={"payment_id": str(payment.id), "error": str(exc)},
                    exc_info=True,
                )
                continue

            if bunq_payment:
                logger.info(
                    "reconciliation.ambiguous_payment_recovered",
                    extra={
                        "payment_id": str(payment.id),
                        "bunq_payment_id": payment.bunq_payment_id,
                    },
                )
                machine = PaymentStateMachine(payment)
                # FIX: previously wrote payment.state = PROCESSING directly,
                # bypassing the state machine entirely.  This could produce
                # invalid transitions that the machine would have rejected.
                # Correct path: FAILED → PROCESSING → SUBMITTED using the
                # machine for every step so invariants are enforced.
                if machine.can_transition_to(PaymentState.PROCESSING):
                    machine.transition_to(PaymentState.PROCESSING)
                elif machine.can_transition_to(PaymentState.SUBMITTED):
                    # Payment is already in a state from which SUBMITTED is
                    # reachable (e.g. it was set to PROCESSING by a concurrent
                    # worker that then crashed before writing SUBMITTED).
                    pass
                else:
                    logger.warning(
                        "reconciliation.ambiguous_recovery_skipped",
                        extra={
                            "payment_id": str(payment.id),
                            "current_state": payment.state,
                            "reason": "Cannot transition to PROCESSING or SUBMITTED from current state",
                        },
                    )
                    continue

                if machine.can_transition_to(PaymentState.SUBMITTED):
                    machine.transition_to(
                        PaymentState.SUBMITTED,
                        bunq_payment_id=payment.bunq_payment_id,
                    )
                # record_payment_sent is idempotent — safe to call even if the
                # original worker already wrote partial ledger entries.
                await ledger.record_payment_sent(
                    payment_id=payment.id,
                    from_account_id=payment.from_account_id,
                    amount=payment.amount,
                    currency=payment.currency,
                    description=f"Recovered ambiguous payment: {payment.bunq_payment_id}",
                )
            else:
                payment.last_error = payment.last_error.replace("AMBIGUOUS: ", "")
                logger.info(
                    "reconciliation.ambiguous_payment_not_found",
                    extra={"payment_id": str(payment.id)},
                )

        await db.flush()
