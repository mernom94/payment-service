"""
tests/factories/payment_factory.py — Factory for Payment and Outbox ORM objects.

All test data creation for the payment domain must go through this factory.
Tests must never instantiate Payment or Outbox directly.

Design principles:
  - Every factory method accepts an explicit db_session.
  - Sensible defaults allow minimal call-sites.
  - Fully refreshed ORM objects are returned so attribute access never hits
    a detached-instance error.
  - Foreign-key dependencies (Outbox → Payment) are handled internally.
"""

import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import OutboxStatus, PaymentState
from app.domain.payments.models import Payment
from app.infrastructure.db.outbox import Outbox


class PaymentFactory:
    """
    Creates fully-persisted Payment rows for use in tests.

    Usage (minimal):
        payment = await PaymentFactory.create(db_session)

    Usage (custom):
        payment = await PaymentFactory.create(
            db_session,
            state=PaymentState.SUBMITTED,
            bunq_payment_id="bunq-42",
            amount=Decimal("100.00"),
        )
    """

    @staticmethod
    async def create(
        db: AsyncSession,
        *,
        external_id: Optional[str] = None,
        from_account_id: str = "test-account-001",
        to_iban: str = "NL02ABNA0123456789",
        amount: Decimal = Decimal("42.50"),
        currency: str = "EUR",
        description: Optional[str] = "Test payment",
        state: PaymentState = PaymentState.PENDING,
        bunq_payment_id: Optional[str] = None,
        retry_count: int = 0,
        last_error: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> Payment:
        """
        Persist and return a Payment row.

        A unique external_id is generated automatically if not supplied, which
        prevents accidental uniqueness collisions between tests.
        """
        payment = Payment(
            id=uuid.uuid4(),
            external_id=external_id or f"test-ext-{uuid.uuid4().hex[:12]}",
            from_account_id=from_account_id,
            to_iban=to_iban,
            amount=amount,
            currency=currency,
            description=description,
            state=state,
            bunq_payment_id=bunq_payment_id,
            retry_count=retry_count,
            last_error=last_error,
            correlation_id=correlation_id,
        )
        db.add(payment)
        await db.flush()
        await db.refresh(payment)
        return payment

    @staticmethod
    async def create_submitted(
        db: AsyncSession,
        *,
        bunq_payment_id: str = "bunq-99001",
        **kwargs,
    ) -> Payment:
        """Convenience builder: a Payment that has been submitted to bunq."""
        return await PaymentFactory.create(
            db,
            state=PaymentState.SUBMITTED,
            bunq_payment_id=bunq_payment_id,
            **kwargs,
        )

    @staticmethod
    async def create_confirmed(db: AsyncSession, **kwargs) -> Payment:
        """Convenience builder: a Payment that has been confirmed."""
        return await PaymentFactory.create(
            db,
            state=PaymentState.CONFIRMED,
            **kwargs,
        )

    @staticmethod
    async def create_failed(
        db: AsyncSession,
        *,
        last_error: str = "Simulated failure",
        retry_count: int = 1,
        **kwargs,
    ) -> Payment:
        """Convenience builder: a Payment in a FAILED state."""
        return await PaymentFactory.create(
            db,
            state=PaymentState.FAILED,
            last_error=last_error,
            retry_count=retry_count,
            **kwargs,
        )


class OutboxFactory:
    """
    Creates fully-persisted Outbox rows for use in tests.

    An Outbox record always references a Payment. If no payment_id is provided,
    a Payment row is created automatically to satisfy the foreign key constraint.
    """

    @staticmethod
    async def create(
        db: AsyncSession,
        *,
        payment: Optional[Payment] = None,
        status: OutboxStatus = OutboxStatus.PENDING,
        retry_count: int = 0,
        last_error: Optional[str] = None,
    ) -> Outbox:
        """
        Persist and return an Outbox row.

        If no payment is supplied, one is created automatically.
        """
        if payment is None:
            payment = await PaymentFactory.create(db)

        outbox = Outbox(
            id=uuid.uuid4(),
            payment_id=payment.id,
            status=status,
            retry_count=retry_count,
            last_error=last_error,
            payload={
                "payment_id": str(payment.id),
                "external_id": payment.external_id,
                "from_account_id": payment.from_account_id,
                "to_iban": payment.to_iban,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "description": payment.description,
            },
        )
        db.add(outbox)
        await db.flush()
        await db.refresh(outbox)
        return outbox
