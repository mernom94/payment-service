"""
app/infrastructure/bunq/webhook_adapter.py — bunq webhook payload normaliser.

Translates raw bunq notification payloads into normalised internal dicts
that the domain layer can work with regardless of which bunq event type
arrived.

bunq webhook docs: https://doc.bunq.com/#/notification-filter
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BunqWebhookAdapter:
    """
    Normalises raw bunq webhook payloads.

    bunq sends different shapes for different event types. This adapter
    provides a consistent interface to the domain layer.
    """

    def normalise(self, raw_payload: dict[str, Any]) -> dict[str, Any]:
        """
        Normalise a raw bunq webhook payload.

        Returns a dict with:
          event_type  — string identifier (PAYMENT, MUTATION, etc.)
          event_id    — stable deduplication key
          object_id   — ID of the bunq object affected
          status      — status of the object (PENDING, ACCEPTED, REJECTED, etc.)
          raw         — the original payload (always preserved)
        """
        notification_type = raw_payload.get("NotificationType", "UNKNOWN").upper()

        normalised: dict[str, Any] = {
            "event_type": notification_type,
            "raw": raw_payload,
            "event_id": None,
            "object_id": None,
            "status": None,
        }

        if notification_type == "PAYMENT":
            normalised.update(self._normalise_payment(raw_payload))
        elif notification_type == "MUTATION":
            normalised.update(self._normalise_mutation(raw_payload))
        elif notification_type == "PAYMENT_BATCH":
            normalised.update(self._normalise_payment_batch(raw_payload))
        else:
            logger.info(
                "webhook.adapter.unhandled_type",
                extra={"notification_type": notification_type},
            )

        return normalised

    # ── Event-specific normalisers ────────────────────────────────────────────

    def _normalise_payment(self, payload: dict) -> dict:
        payment = payload.get("Payment", {})
        return {
            "event_id": f"PAYMENT:{payment.get('id')}",
            "object_id": str(payment.get("id", "")),
            "status": payment.get("status", "").upper(),
            "amount": payment.get("amount", {}).get("value"),
            "currency": payment.get("amount", {}).get("currency"),
            "created": payment.get("created"),
            "updated": payment.get("updated"),
            "description": payment.get("description"),
            "bunq_account_id": str(payment.get("monetary_account_id", "")),
        }

    def _normalise_mutation(self, payload: dict) -> dict:
        mutation = payload.get("Payment", payload.get("Mutation", {}))
        return {
            "event_id": f"MUTATION:{mutation.get('id')}",
            "object_id": str(mutation.get("id", "")),
            "status": mutation.get("status", "").upper(),
        }

    def _normalise_payment_batch(self, payload: dict) -> dict:
        batch = payload.get("PaymentBatch", {})
        return {
            "event_id": f"PAYMENT_BATCH:{batch.get('id')}",
            "object_id": str(batch.get("id", "")),
            "status": "BATCH",
        }

    # ── Utility ───────────────────────────────────────────────────────────────

    @staticmethod
    def extract_bunq_payment_id(payload: dict[str, Any]) -> Optional[str]:
        """Extract the bunq payment ID from a raw webhook payload."""
        payment = payload.get("Payment", {})
        pid = payment.get("id")
        return str(pid) if pid else None
