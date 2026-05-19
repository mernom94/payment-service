"""
tests/unit/test_validators.py — Unit tests for payment input validators.

Tests IBAN validation (format + MOD-97), currency filtering, and amount checks.
All pure functions — no I/O.
"""

from decimal import Decimal

import pytest

from app.core.exceptions import PaymentValidationError
from app.domain.payments.validators import (
    validate_amount,
    validate_currency,
    validate_iban,
)


class TestValidateIBAN:
    # Known-valid IBANs from IBAN test vectors.
    @pytest.mark.parametrize(
        "iban",
        [
            "NL02ABNA0123456789",
            "DE89370400440532013000",
            "GB29NWBK60161331926819",
            "FR7630006000011234567890189",
            "BE68539007547034",
        ],
    )
    def test_valid_ibans(self, iban):
        result = validate_iban(iban)
        assert result == iban.upper().replace(" ", "")

    def test_normalises_lowercase(self):
        result = validate_iban("nl02abna0123456789")
        assert result == "NL02ABNA0123456789"

    def test_strips_spaces(self):
        result = validate_iban("NL02 ABNA 0123 4567 89")
        assert result == "NL02ABNA0123456789"

    def test_invalid_format_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_iban("NOTANIBAN")

    def test_wrong_length_raises(self):
        # NL IBANs must be 18 chars; this is 17.
        with pytest.raises(PaymentValidationError):
            validate_iban("NL02ABNA012345678")  # 17 chars

    def test_bad_check_digits_raises(self):
        # Change the check digits of a valid IBAN.
        with pytest.raises(PaymentValidationError):
            validate_iban("NL98ABNA0123456789")  # Check digits 99 are invalid here.

    def test_empty_string_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_iban("")


class TestValidateCurrency:
    def test_supported_currency_passes(self):
        assert validate_currency("EUR") == "EUR"
        assert validate_currency("USD") == "USD"

    def test_lowercases_normalised(self):
        assert validate_currency("eur") == "EUR"

    def test_unsupported_currency_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_currency("XYZ")

    def test_empty_string_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_currency("")


class TestValidateAmount:
    def test_valid_positive_amount(self):
        assert validate_amount(Decimal("10.00")) == Decimal("10.00")

    def test_one_decimal_place_passes(self):
        assert validate_amount(Decimal("10.5")) == Decimal("10.5")

    def test_zero_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_amount(Decimal("0.00"))

    def test_negative_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_amount(Decimal("-5.00"))

    def test_too_many_decimal_places_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_amount(Decimal("10.001"))

    def test_exceeds_max_raises(self):
        with pytest.raises(PaymentValidationError):
            validate_amount(Decimal("999999.99"))

    def test_exactly_max_passes(self):
        # MAX_PAYMENT_AMOUNT is "100000.00" in test config.
        amount = Decimal("100000.00")
        assert validate_amount(amount) == amount

    def test_tiny_valid_amount(self):
        assert validate_amount(Decimal("0.01")) == Decimal("0.01")
