"""
app/core/exceptions.py — Domain exception hierarchy.

All exceptions raised inside the application extend from OrchestratorError.
FastAPI exception handlers in deps.py translate these into HTTP responses
so that HTTP status codes are never chosen inside domain or infrastructure
code — only at the API boundary.
"""

from typing import Optional


class OrchestratorError(Exception):
    """Base class for all application errors."""

    def __init__(self, message: str, *, detail: Optional[str] = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail


# ── Payment errors ────────────────────────────────────────────────────────────


class PaymentNotFoundError(OrchestratorError):
    """No payment with the given ID exists."""


class DuplicatePaymentError(OrchestratorError):
    """
    A payment with this external_id already exists.

    Carries the existing payment object so that the exception handler can
    return a full PaymentResponse without an extra DB round-trip (issue #5).
    """

    def __init__(self, external_id: str, existing_payment=None) -> None:
        super().__init__(
            f"Duplicate payment request: external_id={external_id!r} already exists."
        )
        self.external_id = external_id
        self.existing_payment = existing_payment  # May be None if not available.


class InvalidPaymentStateError(OrchestratorError):
    """
    A state transition was attempted that is not permitted by the state machine.
    """

    def __init__(self, current: str, attempted: str) -> None:
        super().__init__(
            f"Cannot transition payment from {current!r} to {attempted!r}."
        )
        self.current = current
        self.attempted = attempted


class PaymentValidationError(OrchestratorError):
    """Input validation failed (bad IBAN, unsupported currency, etc.)."""


# ── Ledger errors ─────────────────────────────────────────────────────────────


class LedgerImbalanceError(OrchestratorError):
    """
    A ledger write was attempted that would leave the books unbalanced.
    This is a critical invariant violation — it should never happen in
    normal operation and should page on-call when it does.
    """

    def __init__(self, imbalance: str) -> None:
        super().__init__(
            f"CRITICAL: ledger imbalance detected. Net = {imbalance}. "
            "Transaction aborted."
        )
        self.imbalance = imbalance


class LedgerEntryNotFoundError(OrchestratorError):
    """No ledger entry with the given ID exists."""


# ── Webhook errors ────────────────────────────────────────────────────────────


class DuplicateWebhookError(OrchestratorError):
    """This webhook event_id has already been processed (idempotent skip)."""

    def __init__(self, event_id: str) -> None:
        super().__init__(f"Webhook event_id={event_id!r} already processed.")
        self.event_id = event_id


class WebhookProcessingError(OrchestratorError):
    """Unrecoverable error processing a webhook event."""


# ── bunq / external API errors ────────────────────────────────────────────────


class BunqSessionError(OrchestratorError):
    """bunq session is missing, expired, or invalid."""


class BunqAPIError(OrchestratorError):
    """
    bunq returned a non-success HTTP response.
    Carries the HTTP status and bunq error body for retry/logging decisions.
    """

    def __init__(self, status_code: int, bunq_message: str) -> None:
        super().__init__(
            f"bunq API error {status_code}: {bunq_message}",
            detail=bunq_message,
        )
        self.status_code = status_code
        self.bunq_message = bunq_message


class BunqNetworkError(OrchestratorError):
    """Network-level failure communicating with bunq (timeout, DNS, etc.)."""


class BunqPaymentAmbiguousError(OrchestratorError):
    """
    We sent a payment to bunq but received a network error before getting
    a response. The payment may or may not have been created.
    This triggers the reconciliation path.
    """


class BunqWebhookSignatureError(OrchestratorError):
    """
    The X-Bunq-Server-Signature header on an inbound webhook failed RSA-SHA256
    verification against the server public key captured during POST /installation.

    This means either:
      - the payload was tampered with in transit, or
      - the request did not originate from bunq.

    The webhook must be rejected. Do NOT process domain logic (state transitions,
    ledger writes) for a webhook that fails this check.
    """


# ── Infrastructure errors ─────────────────────────────────────────────────────


class IdempotencyLockError(OrchestratorError):
    """Could not acquire the idempotency lock for this external_id."""


class ReconciliationError(OrchestratorError):
    """Drift detected between internal ledger and bunq balance."""

    def __init__(self, account_id: str, internal: str, external: str) -> None:
        super().__init__(
            f"Reconciliation drift on account {account_id}: "
            f"internal={internal}, bunq={external}"
        )
        self.account_id = account_id
        self.internal = internal
        self.external = external
