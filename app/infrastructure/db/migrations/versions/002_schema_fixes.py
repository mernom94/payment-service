"""
002_schema_fixes.py — Schema corrections from code review.

Changes:
  1. outbox: add composite index (status, next_attempt_at) for the worker's
     primary query.  The prior separate indexes on status and payment_id
     did not cover the combined filter the worker uses every tick.

  2. outbox: fix payload column nullability to NOT NULL (was incorrectly
     nullable=True in 001, conflicting with the ORM model's nullable=False).

  3. webhook_events: add retry_count (INT NOT NULL DEFAULT 0) and
     retry_after (TIMESTAMPTZ NULL) columns to support the retry/DLQ
     implementation that replaces the previous "log and drop" behaviour.

  4. webhook_events: add RETRY_PENDING to the status CHECK constraint.

  5. webhook_events: add composite index (status, retry_after) for the
     webhook worker's RETRY_PENDING scan.

  6. bunq_sessions: add unique constraint on session_token to prevent
     duplicate inserts from concurrent bootstrap calls.
"""

import sqlalchemy as sa
from alembic import op

revision = "002_schema_fixes"
down_revision = "001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. Composite index on outbox (status, next_attempt_at) ───────────────
    # The payment worker's main query is:
    #   WHERE status = 'PENDING' AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    # A partial index on the PENDING subset with next_attempt_at is optimal.
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_outbox_status_next_attempt
        ON outbox (next_attempt_at)
        WHERE status = 'PENDING'
    """)

    # ── 2. Fix outbox.payload nullability ─────────────────────────────────────
    # Migration 001 created payload as nullable; the ORM model declares it
    # NOT NULL.  Align the DB with the model.
    op.execute("""
        ALTER TABLE outbox
        ALTER COLUMN payload SET NOT NULL
    """)

    # ── 3. Add retry tracking columns to webhook_events ──────────────────────
    op.add_column(
        "webhook_events",
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "webhook_events",
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
    )

    # ── 4. Expand webhook_events status CHECK to include RETRY_PENDING ────────
    op.execute("ALTER TABLE webhook_events DROP CONSTRAINT ck_webhook_events_status")
    op.execute("""
        ALTER TABLE webhook_events
        ADD CONSTRAINT ck_webhook_events_status
        CHECK (status IN ('RECEIVED', 'QUEUED', 'PROCESSED', 'FAILED', 'SKIPPED', 'RETRY_PENDING'))
    """)

    # ── 5. Index for RETRY_PENDING scan ───────────────────────────────────────
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_webhook_events_retry_pending
        ON webhook_events (retry_after)
        WHERE status = 'RETRY_PENDING'
    """)

    # ── 6. Unique constraint on bunq_sessions.session_token ──────────────────
    # Prevents duplicate rows from concurrent bootstrap calls.
    op.create_unique_constraint(
        "uq_bunq_sessions_token",
        "bunq_sessions",
        ["session_token"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_bunq_sessions_token", "bunq_sessions", type_="unique")

    op.execute("DROP INDEX IF EXISTS ix_webhook_events_retry_pending")

    op.execute("ALTER TABLE webhook_events DROP CONSTRAINT ck_webhook_events_status")
    op.execute("""
        ALTER TABLE webhook_events
        ADD CONSTRAINT ck_webhook_events_status
        CHECK (status IN ('RECEIVED', 'QUEUED', 'PROCESSED', 'FAILED', 'SKIPPED'))
    """)

    op.drop_column("webhook_events", "retry_after")
    op.drop_column("webhook_events", "retry_count")

    op.execute("ALTER TABLE outbox ALTER COLUMN payload DROP NOT NULL")

    op.execute("DROP INDEX IF EXISTS ix_outbox_status_next_attempt")


def _add_private_key_encryption_migration():
    """
    PRODUCTION REQUIREMENT — NOT APPLIED AUTOMATICALLY.

    The RSA private key in bunq_sessions.private_key_pem is stored as
    plaintext PEM.  In production this MUST be encrypted at rest using
    a KMS-managed key (AWS KMS, GCP Cloud KMS, HashiCorp Vault, etc.)
    before go-live.

    Recommended approach:
      1. On write: encrypt the PEM bytes using the KMS envelope encryption
         pattern (generate a DEK, encrypt the PEM with AES-256-GCM, encrypt
         the DEK with the KMS CMK, store both ciphertexts).
      2. On read: decrypt DEK via KMS, decrypt PEM with DEK.
      3. Rotate the CMK on schedule; re-encrypt all DEKs without touching
         the application private key.

    Until this is implemented the service MUST run with the database
    access restricted to the application service account only, and
    database-level encryption (pg_tde or cloud-provider transparent
    encryption) MUST be enabled.

    Ref: https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html
    """
    pass
