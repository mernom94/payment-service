"""
app/domain/payments/validators.py — Payment input validation.

Pure functions — no I/O, no DB access, easy to unit test.
All raise PaymentValidationError on failure.
"""

import re
from decimal import Decimal

from app.core.config import get_settings
from app.core.exceptions import PaymentValidationError


# ── IBAN ──────────────────────────────────────────────────────────────────────

# IBAN: 2-letter country code + 2 check digits + up to 30 alphanumeric chars.
_IBAN_PATTERN = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$")

# IBAN country code → expected total length.
_IBAN_LENGTHS: dict[str, int] = {
    "AL": 28,
    "AD": 24,
    "AT": 20,
    "AZ": 28,
    "BH": 22,
    "BY": 28,
    "BE": 16,
    "BA": 20,
    "BR": 29,
    "BG": 22,
    "CR": 22,
    "HR": 21,
    "CY": 28,
    "CZ": 24,
    "DK": 18,
    "DO": 28,
    "EG": 29,
    "SV": 28,
    "EE": 20,
    "FO": 18,
    "FI": 18,
    "FR": 27,
    "GE": 22,
    "DE": 22,
    "GI": 23,
    "GR": 27,
    "GL": 18,
    "GT": 28,
    "HU": 28,
    "IS": 26,
    "IQ": 23,
    "IE": 22,
    "IL": 23,
    "IT": 27,
    "JO": 30,
    "KZ": 20,
    "XK": 20,
    "KW": 30,
    "LV": 21,
    "LB": 28,
    "LI": 21,
    "LT": 20,
    "LU": 20,
    "MT": 31,
    "MR": 27,
    "MU": 30,
    "MD": 24,
    "MC": 27,
    "ME": 22,
    "NL": 18,
    "MK": 19,
    "NO": 15,
    "PK": 24,
    "PS": 29,
    "PL": 28,
    "PT": 25,
    "QA": 29,
    "RO": 24,
    "LC": 32,
    "SM": 27,
    "ST": 25,
    "SA": 24,
    "RS": 22,
    "SC": 31,
    "SK": 24,
    "SI": 19,
    "ES": 24,
    "SD": 18,
    "SE": 24,
    "CH": 21,
    "TL": 23,
    "TN": 24,
    "TR": 26,
    "UA": 29,
    "AE": 23,
    "GB": 22,
    "VA": 22,
    "VG": 24,
    "YE": 30,
}


def validate_iban(iban: str) -> str:
    """
    Validate an IBAN string.

    Checks:
      - Format matches the IBAN regex.
      - Length matches the expected length for the country code.
      - MOD-97 check digit validation (ISO 7064).

    Returns the normalised (uppercase, no spaces) IBAN.
    Raises PaymentValidationError if invalid.
    """
    iban = iban.strip().replace(" ", "").upper()

    if not _IBAN_PATTERN.fullmatch(iban):
        raise PaymentValidationError(f"Invalid IBAN format: {iban!r}")

    country = iban[:2]
    expected_length = _IBAN_LENGTHS.get(country)
    if expected_length and len(iban) != expected_length:
        raise PaymentValidationError(
            f"IBAN {iban!r}: expected length {expected_length} for country {country}, "
            f"got {len(iban)}."
        )

    # MOD-97 check: move first 4 chars to end, convert letters to digits, check mod 97 == 1.
    rearranged = iban[4:] + iban[:4]
    numeric = "".join(str(ord(c) - 55) if c.isalpha() else c for c in rearranged)
    if int(numeric) % 97 != 1:
        raise PaymentValidationError(
            f"IBAN {iban!r} failed MOD-97 check digit validation."
        )

    return iban


# ── Currency ──────────────────────────────────────────────────────────────────


def validate_currency(currency: str) -> str:
    """
    Validate ISO 4217 currency code against the supported currencies list.

    Raises PaymentValidationError if not supported.
    """
    currency = currency.strip().upper()
    if currency not in get_settings().SUPPORTED_CURRENCIES:
        raise PaymentValidationError(
            f"Currency {currency!r} is not supported. "
            f"Supported: {get_settings().SUPPORTED_CURRENCIES}"
        )
    return currency


# ── Amount ────────────────────────────────────────────────────────────────────


def validate_amount(amount: Decimal) -> Decimal:
    """
    Validate payment amount.

    Rules:
      - Must be positive.
      - Must have at most 2 decimal places.
      - Must not exceed the configured safety cap.
    """
    if amount <= Decimal("0"):
        raise PaymentValidationError("Payment amount must be greater than zero.")

    if amount != amount.quantize(Decimal("0.01")):
        raise PaymentValidationError("Amount must have at most 2 decimal places.")

    max_amount = Decimal(get_settings().MAX_PAYMENT_AMOUNT)
    if amount > max_amount:
        raise PaymentValidationError(
            f"Amount {amount} exceeds the maximum allowed payment amount of {max_amount}."
        )

    return amount


# ── Composite validator ───────────────────────────────────────────────────────


def validate_create_payment_request(
    external_id: str,
    from_account_id: str,
    to_iban: str,
    amount: Decimal,
    currency: str,
) -> tuple[str, str, Decimal, str]:
    """
    Run all validators for a create payment request.

    Returns (validated_iban, validated_currency, validated_amount, external_id).
    Raises PaymentValidationError on first failure.
    """
    if not external_id or not external_id.strip():
        raise PaymentValidationError("external_id must not be empty.")

    if not from_account_id or not from_account_id.strip():
        raise PaymentValidationError("from_account_id must not be empty.")

    validated_iban = validate_iban(to_iban)
    validated_currency = validate_currency(currency)
    validated_amount = validate_amount(amount)

    return validated_iban, validated_currency, validated_amount, external_id.strip()
