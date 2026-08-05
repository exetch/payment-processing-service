import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.models import Currency, Payment, PaymentStatus
from app.schemas import PaymentCreate, PaymentOut

VALID = {
    "amount": "10.00",
    "currency": "RUB",
    "webhook_url": "https://receiver.test/hook",
}


def test_minimal_payload_is_accepted() -> None:
    payment = PaymentCreate(**VALID)  # type: ignore[arg-type]
    assert payment.amount == Decimal("10.00")
    assert payment.currency is Currency.RUB
    assert payment.description is None
    assert payment.metadata is None


@pytest.mark.parametrize("amount", ["0", "-1", "-0.01"])
def test_amount_must_be_positive(amount: str) -> None:
    with pytest.raises(ValidationError, match="greater_than"):
        PaymentCreate(**{**VALID, "amount": amount})  # type: ignore[arg-type]


def test_amount_rejects_extra_decimals() -> None:
    with pytest.raises(ValidationError, match="no more than 2 decimal places"):
        PaymentCreate(**{**VALID, "amount": "10.123"})  # type: ignore[arg-type]


@pytest.mark.parametrize("currency", ["GBP", "rub", "", "RUBLE"])
def test_currency_is_restricted_to_three(currency: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID, "currency": currency})  # type: ignore[arg-type]


@pytest.mark.parametrize("url", ["ftp://host/hook", "not-a-url", "", "file:///etc/passwd"])
def test_webhook_url_must_be_http(url: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID, "webhook_url": url})  # type: ignore[arg-type]


def test_description_length_is_bounded() -> None:
    with pytest.raises(ValidationError, match="too_long"):
        PaymentCreate(**{**VALID, "description": "x" * 1025})  # type: ignore[arg-type]


def test_metadata_accepts_nested_objects() -> None:
    payment = PaymentCreate(**{**VALID, "metadata": {"a": {"b": [1, 2]}}})  # type: ignore[arg-type]
    assert payment.metadata == {"a": {"b": [1, 2]}}


def test_payment_out_maps_metadata_column() -> None:
    now = datetime.now(tz=UTC)
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("12.30"),
        currency=Currency.USD,
        description="desc",
        metadata_={"k": "v"},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="key",
        webhook_url="https://receiver.test/hook",
        created_at=now,
        processed_at=now,
        webhook_delivered_at=None,
    )
    dto = PaymentOut.from_model(payment)
    assert dto.payment_id == payment.id
    assert dto.metadata == {"k": "v"}
    assert dto.status is PaymentStatus.SUCCEEDED
    assert dto.webhook_delivered_at is None


def test_amount_is_serialised_as_string_not_float() -> None:
    """Деньги в JSON — строкой: float потерял бы точность."""
    now = datetime.now(tz=UTC)
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("1499.90"),
        currency=Currency.RUB,
        description=None,
        metadata_=None,
        status=PaymentStatus.PENDING,
        idempotency_key="key",
        webhook_url="https://receiver.test/hook",
        created_at=now,
        processed_at=None,
        webhook_delivered_at=None,
    )
    dumped = PaymentOut.from_model(payment).model_dump(mode="json")
    assert dumped["amount"] == "1499.90"
