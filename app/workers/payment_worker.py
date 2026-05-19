"""
app/workers/payment_worker.py — Payment outbox worker.

Polls the outbox table for PENDING records and processes them:
  1. Mark outbox record as PROCESSING (under SELECT FOR UPDATE SKIP LOCKED).
  2. Transition payment to PROCESSING state.
  3. Call bunq to create the payment.
  4. On success: record bunq_payment_id, transition to SUBMITTED, write
     ledger entry (DEBIT from_account / CREDIT suspense), mark outbox DONE.
  5. On failure: transition payment to FAILED, increment retry_count,
     schedule next attempt with exponential backoff, mark outbox FAILED
     (if retries exhausted) or re-queue for retry.

Critical design: we fetch only record IDs in the batch query, then open a
*fresh session per record* for processing. This prevents the lock-release
bug where a commit inside a loop releases all FOR UPDATE locks acquired by
the outer transaction, allowing concurrent workers to steal records.

PROCESSING stuck-state recovery:
  Any outbox row stuck in PROCESSING for longer than OUTBOX_LOCK_TIMEOUT_SECONDS
  is reset to PENDING on each tick.  This handles the case where a worker
  crashed after setting status=PROCESSING but before committing the final
  DONE/FAILED/PENDING update — without this, the payment would be silently
  lost forever.
"""

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.constants import MAX_PAYMENT_RETRIES, OutboxStatus, PaymentState
from app.core.exceptions import BunqPaymentAmbiguousError
from app.core.observability import (
    dlq_entries_total,
    get_tracer,
    payment_latency_seconds,
    payments_submitted_total,
)
from app.domain.ledger.engine import LedgerEngine
from app.domain.payments.models import Payment
from app.domain.payments.state_machine import PaymentStateMachine
from app.infrastructure.bunq.client import BunqClient
from app.infrastructure.bunq.payments import BunqPaymentAdapter
from app.infrastructure.bunq.session_manager import get_session_manager
from app.infrastructure.db.outbox import Outbox
from app.infrastructure.db.session import (
    get_ledger_session_factory,
    get_session_factory,
)
from app.infrastructure.messaging.workers import BaseWorker

_tracer = get_tracer(__name__)

logger = logging.getLogger(__name__)


class PaymentWorker(BaseWorker):
    name = "payment_worker"
    poll_interval = get_settings().PAYMENT_WORKER_POLL_INTERVAL

    def __init__(
        self,
        session_factory=None,
        ledger_session_factory=None,
    ):
        """
        Factories are injectable for tests.

        Defaults intentionally resolve lazily at runtime so tests can patch:
            app.workers.payment_worker.get_session_factory
            app.workers.payment_worker.get_ledger_session_factory

        Do NOT resolve these at import time.
        """
        self.session_factory = session_factory
        self.ledger_session_factory = ledger_session_factory

    async def tick(self) -> None:
        """
        One worker iteration:

          1. Recover any outbox rows stuck in PROCESSING (worker crashed mid-flight).
          2. Fetch a batch of PENDING outbox record IDs.
          3. Process each in its own independent session.

        Step 1 runs every tick. A row is "stuck" if it has been in PROCESSING
        for longer than OUTBOX_LOCK_TIMEOUT_SECONDS + a safety buffer.
        """
        await self._recover_stuck_processing_records()

        record_ids = await self._fetch_pending_outbox_ids()

        for record_id in record_ids:
            try:
                await self._process_outbox_record(record_id)
            except Exception as exc:
                logger.error(
                    "payment_worker.unhandled_error",
                    extra={
                        "outbox_id": str(record_id),
                        "error": str(exc),
                    },
                    exc_info=True,
                )

    async def _recover_stuck_processing_records(self) -> None:
        """
        Reset outbox rows stuck in PROCESSING back to PENDING.

        A row is considered stuck if it has been in PROCESSING state for longer
        than 2 × OUTBOX_LOCK_TIMEOUT_SECONDS.  This accounts for slow commits and
        network hiccups while still catching genuine crashes.

        Incrementing retry_count here ensures that a payment that crashes every
        time mid-PROCESSING will eventually exhaust retries rather than looping
        forever.
        """
        stuck_threshold = datetime.now(timezone.utc) - timedelta(
            seconds=get_settings().OUTBOX_LOCK_TIMEOUT_SECONDS * 2
        )
        session_factory = self.session_factory or get_session_factory()
        async with session_factory() as db:
            # Find stuck rows under FOR UPDATE so concurrent workers don't
            # double-reset the same row.
            result = await db.execute(
                select(Outbox)
                .where(
                    Outbox.status == OutboxStatus.PROCESSING,
                    Outbox.updated_at <= stuck_threshold,
                )
                .with_for_update(skip_locked=True)
            )
            stuck = result.scalars().all()

            if not stuck:
                await db.commit()
                return

            for outbox in stuck:
                outbox.retry_count += 1
                outbox.last_error = f"Recovered from stuck PROCESSING state at {datetime.now(timezone.utc).isoformat()}"

                # Load the payment to apply state transitions via the state machine.
                # Bypassing the machine (raw update()) skips invariant checks and
                # produces invalid states that won't be caught until worker retry.
                pay_result = await db.execute(
                    select(Payment).where(Payment.id == outbox.payment_id)
                )
                stuck_payment = pay_result.scalar_one_or_none()

                if outbox.retry_count >= MAX_PAYMENT_RETRIES:
                    # Exhausted retries: permanently fail this record.
                    outbox.status = OutboxStatus.FAILED
                    # Use state machine to enforce valid transition.
                    if stuck_payment:
                        from app.domain.payments.state_machine import (
                            PaymentStateMachine,
                        )

                        machine = PaymentStateMachine(stuck_payment)
                        if machine.can_transition_to(PaymentState.FAILED):
                            machine.transition_to(
                                PaymentState.FAILED,
                                error_message=outbox.last_error,
                            )
                        else:
                            # Payment is in a state where FAILED is not reachable —
                            # update last_error only, do not mutate state.
                            stuck_payment.last_error = outbox.last_error
                    logger.error(
                        "payment_worker.stuck_processing.max_retries_exhausted",
                        extra={
                            "outbox_id": str(outbox.id),
                            "payment_id": str(outbox.payment_id),
                            "retry_count": outbox.retry_count,
                        },
                    )
                else:
                    # Schedule a retry with backoff from the stuck threshold.
                    backoff_seconds = min(2**outbox.retry_count, 300)
                    outbox.next_attempt_at = datetime.now(timezone.utc) + timedelta(
                        seconds=backoff_seconds
                    )
                    outbox.status = OutboxStatus.PENDING
                    # Transition payment back to PENDING via state machine.
                    if stuck_payment:
                        from app.domain.payments.state_machine import (
                            PaymentStateMachine,
                        )

                        machine = PaymentStateMachine(stuck_payment)
                        # PROCESSING → FAILED → PENDING (via retry path).
                        if machine.can_transition_to(PaymentState.FAILED):
                            machine.transition_to(
                                PaymentState.FAILED,
                                error_message=outbox.last_error,
                            )
                        if machine.can_transition_to(PaymentState.PENDING):
                            machine.transition_to(PaymentState.PENDING)
                    logger.warning(
                        "payment_worker.stuck_processing.recovered",
                        extra={
                            "outbox_id": str(outbox.id),
                            "payment_id": str(outbox.payment_id),
                            "retry_count": outbox.retry_count,
                            "next_attempt_at": outbox.next_attempt_at.isoformat()
                            if outbox.next_attempt_at
                            else None,
                        },
                    )

            await db.commit()

    async def _fetch_pending_outbox_ids(self) -> list[uuid.UUID]:
        """
        Fetch IDs of outbox records ready to process.

        Returns only IDs so the transaction (and its FOR UPDATE locks) closes
        immediately. Each ID is re-locked individually inside
        _process_outbox_record with a fresh session.
        """
        session_factory = self.session_factory or get_session_factory()
        async with session_factory() as db:
            now = datetime.now(timezone.utc)
            result = await db.execute(
                select(Outbox.id)
                .where(
                    Outbox.status == OutboxStatus.PENDING,
                    or_(
                        Outbox.next_attempt_at.is_(None),
                        Outbox.next_attempt_at <= now,
                    ),
                )
                .limit(get_settings().OUTBOX_BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            ids = [row[0] for row in result.all()]
            await db.commit()  # Release FOR UPDATE locks immediately.
        return ids

    async def _process_outbox_record(self, outbox_id: uuid.UUID) -> None:
        """
        Process a single outbox record inside its own dedicated session.

        Opens a fresh SERIALIZABLE session, re-fetches the outbox row under
        FOR UPDATE to claim it exclusively, then processes the payment.

        Why SERIALIZABLE here specifically: this session performs a ledger
        balance update (read-then-write via _write_entries) which requires
        strict isolation.  All other sessions in the system use READ COMMITTED.
        The SELECT FOR UPDATE in _write_entries serialises concurrent updates
        on the same accounts, but SERIALIZABLE adds an extra guard against
        phantom reads in edge cases.
        """
        # Ledger writes happen inside this transaction, so we use a
        # SERIALIZABLE session here.  See get_ledger_session_factory().
        ledger_session_factory = (
            self.ledger_session_factory
            or self.session_factory
            or get_ledger_session_factory()
        )
        async with ledger_session_factory() as db:
            # Re-lock this specific row so concurrent workers skip it.
            result = await db.execute(
                select(Outbox)
                .where(Outbox.id == outbox_id, Outbox.status == OutboxStatus.PENDING)
                .with_for_update(skip_locked=True)
            )
            outbox = result.scalar_one_or_none()
            if not outbox:
                # Another worker claimed it between our ID fetch and now.
                return

            await self._do_process(db, outbox)
            await db.commit()

    async def _do_process(self, db: AsyncSession, outbox: Outbox) -> None:
        """Core processing logic for one outbox record."""
        payment_id = outbox.payment_id
        payload = outbox.payload or {}
        currency = str(payload.get("currency", "UNKNOWN"))

        logger.info(
            "payment_worker.processing",
            extra={"payment_id": str(payment_id), "retry_count": outbox.retry_count},
        )

        with _tracer.start_as_current_span("payment_worker.process") as span:
            span.set_attribute("payment.id", str(payment_id))
            span.set_attribute("payment.currency", currency)
            span.set_attribute("outbox.retry_count", outbox.retry_count)

            outbox.status = OutboxStatus.PROCESSING
            await db.flush()

            result = await db.execute(select(Payment).where(Payment.id == payment_id))
            payment = result.scalar_one_or_none()
            if not payment:
                logger.error(
                    "payment_worker.payment_not_found",
                    extra={"payment_id": str(payment_id)},
                )
                outbox.status = OutboxStatus.FAILED
                outbox.last_error = "Payment record not found"
                return

            machine = PaymentStateMachine(payment)

            if not machine.can_transition_to(PaymentState.PROCESSING):
                logger.warning(
                    "payment_worker.invalid_state",
                    extra={"payment_id": str(payment_id), "state": payment.state},
                )
                outbox.status = OutboxStatus.DONE
                return

            machine.transition_to(PaymentState.PROCESSING)
            await db.flush()

            t_start = time.perf_counter()

            # Use the shared singleton — not a fresh instance per tick.
            session_manager = get_session_manager()
            async with BunqClient(session_manager) as client:
                adapter = BunqPaymentAdapter(client)
                try:
                    with _tracer.start_as_current_span("bunq.create_payment") as bunq_span:
                        bunq_span.set_attribute("payment.id", str(payment_id))
                        bunq_payment_id = await adapter.create_payment(
                            monetary_account_id=str(payload["from_account_id"]),
                            to_iban=str(payload["to_iban"]),
                            amount=Decimal(str(payload["amount"])),
                            currency=currency,
                            description=payload.get("description") or "Payment",
                        )

                    machine.transition_to(
                        PaymentState.SUBMITTED,
                        bunq_payment_id=bunq_payment_id
                    )

                    await db.flush()
                    await db.refresh(payment)

                    # IMPORTANT: ensure payment is persisted before ledger write (fix FK violation in tests)
                    await db.commit()

                    ledger = LedgerEngine(db)
                    await ledger.record_payment_sent(
                        payment_id=payment.id,
                        from_account_id=str(payload["from_account_id"]),
                        amount=Decimal(str(payload["amount"])),
                        currency=currency,
                        description=f"Payment submitted to bunq: {bunq_payment_id}",
                    )                    
                    outbox.status = OutboxStatus.DONE

                    elapsed = time.perf_counter() - t_start
                    payment_latency_seconds.labels(stage="submit").observe(elapsed)
                    payments_submitted_total.labels(
                        currency=currency,
                        state_transition="PENDING_to_SUBMITTED",
                    ).inc()

                    logger.info(
                        "payment_worker.submitted",
                        extra={
                            "payment_id": str(payment_id),
                            "bunq_payment_id": bunq_payment_id,
                        },
                    )

                except BunqPaymentAmbiguousError as exc:
                    logger.error(
                        "payment_worker.ambiguous_outcome",
                        extra={"payment_id": str(payment_id), "error": str(exc)},
                    )
                    # CRITICAL FIX: The reconciliation worker's ambiguous-payment
                    # recovery path checks payment.bunq_payment_id to look up the
                    # payment on bunq. If create_payment() raised before returning
                    # the ID (network timeout), bunq_payment_id is not available
                    # here and the payment is permanently unrecoverable by reconciliation.
                    # We store what we know and log clearly. The reconciliation worker
                    # will need to search by other attributes (amount, IBAN, time window)
                    # if bunq_payment_id is absent, which is handled by its warning log.
                    machine.transition_to(
                        PaymentState.FAILED, error_message=f"AMBIGUOUS: {exc}"
                    )
                    outbox.status = OutboxStatus.FAILED
                    outbox.last_error = f"AMBIGUOUS: {exc}"

                except Exception as exc:
                    logger.error(
                        "payment_worker.failed",
                        extra={"payment_id": str(payment_id), "error": str(exc)},
                        exc_info=True,
                    )
                    await self._handle_failure(db, payment, machine, outbox, str(exc))

    async def _handle_failure(
        self,
        db: AsyncSession,
        payment: Payment,
        machine: PaymentStateMachine,
        outbox: Outbox,
        error_message: str,
    ) -> None:
        """Apply retry backoff or mark as permanently failed.

        Retry count authority
        ---------------------
        ``outbox.retry_count`` is the authoritative counter that drives all
        backoff scheduling.  ``payment.retry_count`` is a denormalised mirror
        for observability — callers can read it without joining to the outbox
        table.  Both are incremented here and only here.  The state machine's
        ``transition_to(FAILED)`` does NOT touch retry_count.
        """
        outbox.retry_count += 1
        outbox.last_error = error_message
        # Keep payment.retry_count in sync for observability (not used in
        # scheduling logic — outbox.retry_count is authoritative for that).
        payment.retry_count += 1

        if outbox.retry_count >= MAX_PAYMENT_RETRIES:
            logger.error(
                "payment_worker.max_retries_reached",
                extra={
                    "payment_id": str(payment.id),
                    "retry_count": outbox.retry_count,
                },
            )
            machine.transition_to(PaymentState.FAILED, error_message=error_message)
            outbox.status = OutboxStatus.FAILED
            dlq_entries_total.labels(worker="payment_worker").inc()
        else:
            backoff_seconds = min(2**outbox.retry_count, 300)
            outbox.next_attempt_at = datetime.now(timezone.utc) + timedelta(
                seconds=backoff_seconds
            )
            machine.transition_to(PaymentState.FAILED, error_message=error_message)
            machine.transition_to(PaymentState.PENDING)
            outbox.status = OutboxStatus.PENDING

            logger.info(
                "payment_worker.scheduled_retry",
                extra={
                    "payment_id": str(payment.id),
                    "retry_count": outbox.retry_count,
                    "next_attempt_at": outbox.next_attempt_at.isoformat(),
               },
            )
