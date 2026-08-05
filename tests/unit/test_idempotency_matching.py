"""Сравнение тела повторного запроса с сохранённым платежом."""

from decimal import Decimal
from typing import Any

import pytest

from app.models import Currency, Payment
from app.schemas import PaymentCreate
from app.services.payments import PaymentService

BODY: dict[str, Any] = {
    "amount": "10.00",
    "currency": "RUB",
    "description": "desc",
    "metadata": {"order_id": 1},
    "webhook_url": "https://receiver.test/hook",
}


def _payment(**overrides: Any) -> Payment:
    fields: dict[str, Any] = {
        "amount": Decimal("10.00"),
        "currency": Currency.RUB,
        "description": "desc",
        "metadata_": {"order_id": 1},
        "webhook_url": "https://receiver.test/hook",
    }
    return Payment(**{**fields, **overrides})


def _matches(payment: Payment, body: dict[str, Any] | None = None) -> bool:
    return PaymentService._payloads_match(payment, PaymentCreate(**(body or BODY)))


def test_identical_body_matches() -> None:
    assert _matches(_payment()) is True


def test_trailing_zeros_in_amount_still_match() -> None:
    """Decimal сравнивается по значению: 10.5 и 10.50 — одна сумма."""
    assert _matches(_payment(amount=Decimal("10.5")), {**BODY, "amount": "10.50"}) is True


@pytest.mark.parametrize(
    "override",
    [
        {"amount": Decimal("10.01")},
        {"currency": Currency.USD},
        {"description": "other"},
        {"description": None},
        {"metadata_": {"order_id": 2}},
        {"metadata_": None},
        {"metadata_": {}},
        {"webhook_url": "https://other.test/hook"},
    ],
)
def test_any_differing_field_is_a_conflict(override: dict[str, Any]) -> None:
    assert _matches(_payment(**override)) is False


def test_metadata_key_order_does_not_matter() -> None:
    payment = _payment(metadata_={"b": 2, "a": 1})
    assert _matches(payment, {**BODY, "metadata": {"a": 1, "b": 2}}) is True


def test_url_normalisation_does_not_cause_false_conflict() -> None:
    """HttpUrl приводится к строке — сохранённое значение должно совпасть с ним."""
    body = {**BODY, "webhook_url": "https://receiver.test/hook"}
    stored = str(PaymentCreate(**body).webhook_url)
    assert _matches(_payment(webhook_url=stored), body) is True
