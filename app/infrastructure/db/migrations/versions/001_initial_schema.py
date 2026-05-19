"""
Initial schema migration.

Creates all tables and constraints for the Bunq Payment Orchestrator.

Tables:
  - payments
  - ledger_accounts
  - ledger_entries
  - webhook_events
  - processed_webhook_events
  - outbox
  - bunq_sessions

Includes:
  - DB-level immutability enforcement for ledger_entries
  - Unique constraints for idempotency
  - Partial unique index for debit protection
  - CHECK constraints for state/status validation

Revision: 001_initial_schema
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ──────────────────────────────────────────────────────────────────────────
    # payments
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("from_account_id", sa.String(255), nullable=False),
        sa.Column("to_iban", sa.String(34), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "state",
            sa.String(20),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("bunq_payment_id", sa.String(255), nullable=True),
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("correlation_id", sa.String(64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "state IN ('PENDING', 'PROCESSING', 'SUBMITTED', 'CONFIRMED', 'FAILED')",
            name="ck_payments_state",
        ),
    )

    op.create_unique_constraint(
        "uq_payments_external_id",
        "payments",
        ["external_id"],
    )

    op.create_index(
        "ix_payments_external_id",
        "payments",
        ["external_id"],
    )

    op.create_index(
        "ix_payments_state",
        "payments",
        ["state"],
    )

    op.create_index(
        "ix_payments_bunq_payment_id",
        "payments",
        ["bunq_payment_id"],
    )

    # ──────────────────────────────────────────────────────────────────────────
    # ledger_accounts
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "ledger_accounts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("bunq_account_id", sa.String(255), nullable=False),
        sa.Column(
            "account_type",
            sa.String(20),
            nullable=False,
            server_default="PAYMENT",
        ),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "balance",
            sa.Numeric(precision=18, scale=2),
            nullable=False,
            server_default="0.00",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_unique_constraint(
        "uq_ledger_accounts_bunq_id",
        "ledger_accounts",
        ["bunq_account_id"],
    )

    op.create_index(
        "ix_ledger_accounts_bunq_account_id",
        "ledger_accounts",
        ["bunq_account_id"],
    )

    # ──────────────────────────────────────────────────────────────────────────
    # ledger_entries
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "ledger_entries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "account_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("ledger_accounts.id"),
            nullable=False,
        ),
        sa.Column(
            "payment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("payments.id"),
            nullable=True,
        ),
        sa.Column("entry_type", sa.String(10), nullable=False),
        sa.Column(
            "amount",
            sa.Numeric(precision=18, scale=2),
            nullable=False,
        ),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("transaction_ref", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "ix_ledger_entries_account_id",
        "ledger_entries",
        ["account_id"],
    )

    op.create_index(
        "ix_ledger_entries_payment_id",
        "ledger_entries",
        ["payment_id"],
    )

    op.create_index(
        "ix_ledger_entries_transaction_ref",
        "ledger_entries",
        ["transaction_ref"],
    )

    # Prevent duplicate DEBIT entries for same payment
    op.execute("""
        CREATE UNIQUE INDEX uq_ledger_entries_payment_debit
        ON ledger_entries (payment_id)
        WHERE entry_type = 'DEBIT'
        AND payment_id IS NOT NULL
    """)

    # ──────────────────────────────────────────────────────────────────────────
    # webhook_events
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "webhook_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_id", sa.String(255), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=True),
        sa.Column("raw_body", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="RECEIVED",
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('RECEIVED', 'QUEUED', 'PROCESSED', 'FAILED', 'SKIPPED')",
            name="ck_webhook_events_status",
        ),
    )

    op.create_unique_constraint(
        "uq_webhook_events_event_id",
        "webhook_events",
        ["event_id"],
    )

    op.create_index(
        "ix_webhook_events_status",
        "webhook_events",
        ["status"],
    )

    # ──────────────────────────────────────────────────────────────────────────
    # processed_webhook_events
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "processed_webhook_events",
        sa.Column("event_id", sa.String(255), primary_key=True),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "webhook_event_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # outbox
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "payment_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("payments.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="PENDING",
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=True),
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'DONE', 'FAILED')",
            name="ck_outbox_status",
        ),
    )

    op.create_unique_constraint(
        "uq_outbox_payment_id",
        "outbox",
        ["payment_id"],
    )

    op.create_index(
        "ix_outbox_payment_id",
        "outbox",
        ["payment_id"],
    )

    op.create_index(
        "ix_outbox_status",
        "outbox",
        ["status"],
    )

    # ──────────────────────────────────────────────────────────────────────────
    # bunq_sessions
    # ──────────────────────────────────────────────────────────────────────────
    op.create_table(
        "bunq_sessions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("session_token", sa.String(255), nullable=False),
        sa.Column("user_id", sa.String(255), nullable=True),
        sa.Column("installation_token", sa.String(255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Ledger immutability protections
    # ──────────────────────────────────────────────────────────────────────────
    op.execute("""
        CREATE OR REPLACE FUNCTION prevent_ledger_entry_update()
        RETURNS TRIGGER AS $$
        BEGIN
            RAISE EXCEPTION
                'ledger_entries rows are immutable — use reversal entries instead.';
        END;
        $$ LANGUAGE plpgsql;
    """)

    op.execute("""
        CREATE TRIGGER trg_ledger_entries_no_update
        BEFORE UPDATE ON ledger_entries
        FOR EACH ROW
        EXECUTE FUNCTION prevent_ledger_entry_update();
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION prevent_ledger_entry_delete()
        RETURNS TRIGGER AS $$
        BEGIN
            RAISE EXCEPTION
                'ledger_entries rows are immutable — deletions are not allowed.';
        END;
        $$ LANGUAGE plpgsql;
    """)

    op.execute("""
        CREATE TRIGGER trg_ledger_entries_no_delete
        BEFORE DELETE ON ledger_entries
        FOR EACH ROW
        EXECUTE FUNCTION prevent_ledger_entry_delete();
    """)

    # ──────────────────────────────────────────────────────────────────────────
    # updated_at auto-update triggers (SQLAlchemy onupdate only fires from ORM;
    # direct SQL updates (e.g. bulk update() calls) bypass it).
    # ──────────────────────────────────────────────────────────────────────────
    op.execute("""
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    for tbl in ("payments", "ledger_accounts", "outbox"):
        op.execute(f"""
            CREATE TRIGGER trg_{tbl}_updated_at
            BEFORE UPDATE ON {tbl}
            FOR EACH ROW
            EXECUTE FUNCTION set_updated_at();
        """)


def downgrade() -> None:
    # ──────────────────────────────────────────────────────────────────────────
    # Drop triggers/functions
    # ──────────────────────────────────────────────────────────────────────────
    op.execute("""
        DROP TRIGGER IF EXISTS trg_ledger_entries_no_delete
        ON ledger_entries
    """)

    op.execute("""
        DROP TRIGGER IF EXISTS trg_ledger_entries_no_update
        ON ledger_entries
    """)

    op.execute("""
        DROP FUNCTION IF EXISTS prevent_ledger_entry_delete
    """)

    op.execute("""
        DROP FUNCTION IF EXISTS prevent_ledger_entry_update
    """)

    # Drop partial unique index
    op.execute("""
        DROP INDEX IF EXISTS uq_ledger_entries_payment_debit
    """)

    # Drop tables
    op.drop_table("bunq_sessions")
    op.drop_table("outbox")
    op.drop_table("processed_webhook_events")
    op.drop_table("webhook_events")
    op.drop_table("ledger_entries")
    op.drop_table("ledger_accounts")
    op.drop_table("payments")
