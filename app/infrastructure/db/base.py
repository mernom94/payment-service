"""
app/infrastructure/db/base.py — SQLAlchemy declarative base.

All ORM models must inherit from Base.  import_all_models() must be called
before metadata.create_all() or Alembic autogenerate so every table is
registered with the shared MetaData.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared base class for all ORM models."""

    pass


def import_all_models() -> None:
    """
    Import every ORM model so SQLAlchemy's MetaData is fully populated.

    Call this from Alembic env.py and test fixtures before create_all().
    BunqSession now lives in infrastructure/db/models.py — no longer
    requires importing the full session_manager module.
    """
    from app.domain.payments.models import Payment  # noqa: F401
    from app.domain.ledger.models import LedgerAccount, LedgerEntry  # noqa: F401
    from app.domain.webhooks.models import WebhookEvent, ProcessedWebhookEvent  # noqa: F401
    from app.infrastructure.db.outbox import Outbox  # noqa: F401
    from app.infrastructure.db.models import BunqSession  # noqa: F401
