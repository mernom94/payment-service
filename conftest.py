"""
conftest.py — Root pytest configuration.

Inserts the project root into sys.path and re-exports all shared fixtures so
they are discoverable by every test subdirectory without explicit imports.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest

from tests.fixtures.conftest import (  # noqa: E402, F401
    async_engine,
    bunq_payment_webhook_factory,
    db_session,
    mock_redis,
    redis_store,
)


@pytest.fixture
def valid_bunq_payment_webhook() -> dict:
    return {
        "NotificationUrl": [
            {
                "id": 99001,
                "created": "2024-01-15 12:00:00.000000",
                "updated": "2024-01-15 12:00:00.000000",
                "category": "PAYMENT",
                "event_type": "PAYMENT",
                "object": {
                    "Payment": {
                        "id": 99001,
                        "created": "2024-01-15 12:00:00.000000",
                        "updated": "2024-01-15 12:00:00.000000",
                        "monetary_account_id": 12345,
                        "amount": {"value": "100.00", "currency": "EUR"},
                        "description": "Test payment fixture",
                        "type": "IDEAL",
                        "status": "COMPLETED",
                        "sub_status": "NONE",
                        "alias": {
                            "type": "IBAN",
                            "value": "NL02ABNA0123456789",
                            "name": "Test Sender",
                        },
                        "counterparty_alias": {
                            "type": "IBAN",
                            "value": "NL91ABNA0417164300",
                            "name": "Test Recipient",
                        },
                        "balance_after_mutation": {
                            "value": "900.00",
                            "currency": "EUR",
                        },
                    }
                },
            }
        ]
    }
