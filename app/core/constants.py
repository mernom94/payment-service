"""
app/core/constants.py — Enumerations and shared constants.

Using Python enums (rather than string literals scattered through the
codebase) means typos are caught at import time and IDEs can autocomplete
state names.
"""

from enum import Enum


class PaymentState(str, Enum):
    """
    State machine for a payment lifecycle.

    Valid transitions:
        PENDING     → PROCESSING   (worker picks up outbox record)
        PROCESSING  → SUBMITTED    (bunq accepted the payment)
        PROCESSING  → FAILED       (bunq rejected, or unrecoverable error)
        SUBMITTED   → CONFIRMED    (webhook confirms the payment cleared)
        SUBMITTED   → FAILED       (webhook reports payment rejected)
        FAILED      → PENDING      (manual or automatic retry)

    Terminal states: CONFIRMED, FAILED (after max retries exhausted).
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUBMITTED = "SUBMITTED"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


# Which transitions are allowed. Key = current state, value = allowed next states.
VALID_PAYMENT_TRANSITIONS: dict[PaymentState, set[PaymentState]] = {
    PaymentState.PENDING: {PaymentState.PROCESSING},
    PaymentState.PROCESSING: {PaymentState.SUBMITTED, PaymentState.FAILED},
    PaymentState.SUBMITTED: {PaymentState.CONFIRMED, PaymentState.FAILED},
    PaymentState.CONFIRMED: set(),  # Terminal — no transitions out
    # FAILED → PENDING: normal retry path (outbox worker re-schedules).
    # FAILED → PROCESSING: reconciliation recovery — an ambiguous payment
    #   (submit timed out but bunq confirms it went through) needs to be
    #   moved to PROCESSING → SUBMITTED without going through PENDING again.
    PaymentState.FAILED: {PaymentState.PENDING, PaymentState.PROCESSING},
}


class LedgerEntryType(str, Enum):
    """Double-entry accounting entry types."""

    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


class AccountType(str, Enum):
    """Internal account classification (mirrors bunq account types)."""

    PAYMENT = "PAYMENT"  # Sending/receiving monetary account
    FEE = "FEE"  # Fee collection account
    SUSPENSE = "SUSPENSE"  # Holding account for ambiguous/pending amounts


class WebhookEventType(str, Enum):
    """bunq webhook event types we handle."""

    PAYMENT = "PAYMENT"
    PAYMENT_BATCH = "PAYMENT_BATCH"
    MUTATION = "MUTATION"


class WebhookEventStatus(str, Enum):
    """Processing status of a received webhook event."""

    RECEIVED = "RECEIVED"
    QUEUED = "QUEUED"
    PROCESSED = "PROCESSED"
    SKIPPED = "SKIPPED"  # Duplicate — event_id already seen
    FAILED = "FAILED"  # Permanently failed (dead letter)
    RETRY_PENDING = "RETRY_PENDING"  # Scheduled for retry after backoff


class OutboxStatus(str, Enum):
    """Status of an outbox record waiting to be processed by the worker."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    FAILED = "FAILED"


# ── Redis key prefixes ────────────────────────────────────────────────────────

REDIS_IDEMPOTENCY_PREFIX = "idem:"  # idem:{external_id}
REDIS_PAYMENT_LOCK_PREFIX = "lock:pay:"  # lock:pay:{external_id}
REDIS_SESSION_KEY = "bunq:session"
REDIS_REAUTH_LOCK_KEY = "bunq:reauth_lock"

# ── Financial ─────────────────────────────────────────────────────────────────

ZERO_DECIMAL = "0.00"
DECIMAL_PLACES = 2

# ── Retry ─────────────────────────────────────────────────────────────────────

MAX_PAYMENT_RETRIES = 5
MAX_WEBHOOK_RETRIES = 5  # Failed webhook events are retried up to this many times.
OUTBOX_LOCK_TIMEOUT_SECONDS = 60
