"""Инварианты, которые обязана держать сама БД, а не только слой валидации."""

from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Currency, Payment
from app.repositories.uow import UnitOfWork


def _payment(**overrides: Any) -> Payment:
    fields: dict[str, Any] = {
        "amount": Decimal("10.00"),
        "currency": Currency.RUB,
        "description": None,
        "metadata_": None,
        "idempotency_key": "constraint-test",
        "webhook_url": "https://receiver.test/hook",
    }
    return Payment(**{**fields, **overrides})


@pytest.mark.parametrize("amount", [Decimal("0.00"), Decimal("-0.01"), Decimal("-100")])
async def test_non_positive_amount_is_rejected_by_database(
    session_factory: Any, amount: Decimal
) -> None:
    """Запись в обход API не должна пронести отрицательную сумму."""
    with pytest.raises(IntegrityError, match="amount_positive"):
        async with UnitOfWork(session_factory) as uow:
            uow.payments.add(_payment(amount=amount))
            await uow.flush()


async def test_positive_amount_is_accepted(session_factory: Any) -> None:
    async with UnitOfWork(session_factory) as uow:
        payment = uow.payments.add(_payment(amount=Decimal("0.01")))
        await uow.flush()
        assert payment.id is not None


async def test_idempotency_key_is_unique_at_database_level(session_factory: Any) -> None:
    async with UnitOfWork(session_factory) as uow:
        uow.payments.add(_payment(idempotency_key="duplicate"))
        await uow.flush()

    with pytest.raises(Exception, match="idempotency_key"):
        async with UnitOfWork(session_factory) as uow:
            uow.payments.add(_payment(idempotency_key="duplicate"))
            await uow.flush()
