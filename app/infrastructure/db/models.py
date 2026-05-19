"""
app/infrastructure/db/models.py — Infrastructure-layer ORM models.

BunqSession lives here (moved from app/infrastructure/bunq/session_manager.py)
so that:
  - Alembic autogenerate discovers it via import_all_models() without needing
    to import the full session_manager module.
  - ORM models are co-located in infrastructure/db/, the conventional home.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.db.base import Base


class BunqSession(Base):
    """
    Single-row ORM model for persisted bunq installation state.

    The table holds exactly one row (id=1), updated in place on every
    re-authentication.
    """

    __tablename__ = "bunq_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_token: Mapped[str] = mapped_column(String(255), nullable=False)
    private_key_pem: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    server_public_key_pem: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    installation_token: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
