"""
app/infrastructure/bunq/payments.py — bunq payment API adapter.

Encapsulates all interaction with the bunq /monetary-account/{id}/payment
endpoint. The rest of the codebase calls this adapter and never knows about
the raw bunq API shape.
"""

import logging
from decimal import Decimal
from typing import Optional

from app.infrastructure.bunq.client import BunqClient

logger = logging.getLogger(__name__)


class BunqPaymentAdapter:
    """
    Adapter for the bunq payment endpoint.

    All methods accept and return domain types (not raw bunq dicts).
    """

    def __init__(self, client: BunqClient) -> None:
        self._client = client

    async def create_payment(
        self,
        *,
        monetary_account_id: str,
        to_iban: str,
        to_name: str = "Beneficiary",
        amount: Decimal,
        currency: str,
        description: str = "Payment",
    ) -> str:
        """
        Create a payment in bunq.

        Returns the bunq payment ID (string) on success.
        Raises BunqAPIError on rejection, BunqPaymentAmbiguousError on timeout.
        """
        payload = {
            "amount": {
                "value": str(amount),
                "currency": currency,
            },
            "counterparty_alias": {
                "type": "IBAN",
                "value": to_iban,
                "name": to_name,
            },
            "description": description,
            "allow_bunqto": False,
        }

        logger.info(
            "bunq.payment.creating",
            extra={
                "account_id": monetary_account_id,
                "to_iban": to_iban,
                "amount": str(amount),
                "currency": currency,
            },
        )

        response = await self._client.post(
            f"/user/me/monetary-account/{monetary_account_id}/payment",
            json=payload,
        )

        bunq_payment_id = self._extract_payment_id(response)

        logger.info(
            "bunq.payment.created",
            extra={
                "bunq_payment_id": bunq_payment_id,
                "account_id": monetary_account_id,
            },
        )

        return bunq_payment_id

    async def get_payment(
        self,
        *,
        monetary_account_id: str,
        bunq_payment_id: str,
    ) -> Optional[dict]:
        """
        Fetch a payment from bunq by ID.

        Used by the reconciliation job and the ambiguous-payment recovery path
        (to check whether a payment that timed out was actually created).

        Returns the bunq payment dict, or None if not found.
        """
        try:
            response = await self._client.get(
                f"/user/me/monetary-account/{monetary_account_id}/payment/{bunq_payment_id}"
            )
            items = response.get("Response", [])
            for item in items:
                if "Payment" in item:
                    return item["Payment"]
            return None
        except Exception as exc:
            logger.warning(
                "bunq.payment.get_failed",
                extra={"bunq_payment_id": bunq_payment_id, "error": str(exc)},
            )
            return None

    async def list_payments(
        self,
        *,
        monetary_account_id: str,
        count: int = 50,
    ) -> list[dict]:
        """
        List recent payments for a monetary account.

        Used by the reconciliation worker to compare against internal ledger.
        """
        response = await self._client.get(
            f"/user/me/monetary-account/{monetary_account_id}/payment",
            params={"count": count},
        )
        items = response.get("Response", [])
        return [item["Payment"] for item in items if "Payment" in item]

    async def get_account_balance(self, monetary_account_id: str) -> Optional[Decimal]:
        """
        Fetch the current balance of a monetary account from bunq.

        Returns the balance as a Decimal, or None if the account is not found.
        """
        try:
            response = await self._client.get(
                f"/user/me/monetary-account/{monetary_account_id}"
            )
            items = response.get("Response", [])
            for item in items:
                for key in ("MonetaryAccountBank", "MonetaryAccountSavings"):
                    if key in item:
                        balance_str = item[key].get("balance", {}).get("value", None)
                        return Decimal(balance_str) if balance_str else None
            return None
        except Exception as exc:
            logger.warning(
                "bunq.account.get_balance_failed",
                extra={"account_id": monetary_account_id, "error": str(exc)},
            )
            return None

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_payment_id(response: dict) -> str:
        """Extract the payment ID from a bunq create payment response."""
        items = response.get("Response", [])
        for item in items:
            if "Id" in item:
                return str(item["Id"]["id"])
        raise ValueError(
            f"bunq create payment response did not contain an Id: {response}"
        )
