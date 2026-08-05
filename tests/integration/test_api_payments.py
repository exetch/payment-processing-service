"""Контракт HTTP-API против реальной БД."""

import asyncio
import uuid
from typing import Any

import pytest
from sqlalchemy import func, select

from app.models import OutboxMessage, Payment, PaymentStatus

PATH = "/api/v1/payments"


async def _count(session_factory: Any, model: type) -> int:
    async with session_factory() as session:
        return (await session.execute(select(func.count()).select_from(model))).scalar_one()


async def test_requires_api_key(api_client: Any, payment_payload: dict[str, Any]) -> None:
    response = await api_client.post(
        PATH, json=payment_payload, headers={"X-API-Key": "", "Idempotency-Key": "k"}
    )
    assert response.status_code == 401


async def test_rejects_wrong_api_key(api_client: Any, payment_payload: dict[str, Any]) -> None:
    response = await api_client.post(
        PATH, json=payment_payload, headers={"X-API-Key": "nope", "Idempotency-Key": "k"}
    )
    assert response.status_code == 401


async def test_idempotency_key_header_is_mandatory(
    api_client: Any, payment_payload: dict[str, Any]
) -> None:
    assert (await api_client.post(PATH, json=payment_payload)).status_code == 422


@pytest.mark.parametrize(
    "override",
    [
        {"amount": "-5"},
        {"amount": "0"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://host/hook"},
        {"amount": "not-a-number"},
    ],
)
async def test_invalid_body_is_rejected(
    api_client: Any, payment_payload: dict[str, Any], override: dict[str, Any]
) -> None:
    response = await api_client.post(
        PATH, json={**payment_payload, **override}, headers={"Idempotency-Key": "k"}
    )
    assert response.status_code == 422


async def test_create_returns_202_with_accepted_payment(
    api_client: Any, payment_payload: dict[str, Any]
) -> None:
    response = await api_client.post(
        PATH, json=payment_payload, headers={"Idempotency-Key": "key-1"}
    )
    assert response.status_code == 202
    body = response.json()
    assert uuid.UUID(body["payment_id"])
    assert body["status"] == PaymentStatus.PENDING.value
    assert body["created_at"]


async def test_create_writes_payment_and_outbox_together(
    api_client: Any, session_factory: Any, payment_payload: dict[str, Any]
) -> None:
    """Суть outbox: событие появляется ровно вместе с платежом."""
    assert await _count(session_factory, Payment) == 0
    assert await _count(session_factory, OutboxMessage) == 0

    await api_client.post(PATH, json=payment_payload, headers={"Idempotency-Key": "key-2"})

    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxMessage) == 1
    async with session_factory() as session:
        message = (await session.execute(select(OutboxMessage))).scalar_one()
        payment = (await session.execute(select(Payment))).scalar_one()
    assert message.routing_key == "payments.new"
    assert message.payload["payment_id"] == str(payment.id)
    assert message.published_at is None


async def test_get_returns_full_card(
    api_client: Any, payment_payload: dict[str, Any]
) -> None:
    created = await api_client.post(
        PATH, json=payment_payload, headers={"Idempotency-Key": "key-3"}
    )
    payment_id = created.json()["payment_id"]

    response = await api_client.get(f"{PATH}/{payment_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "1499.90"
    assert body["currency"] == "RUB"
    assert body["description"] == payment_payload["description"]
    assert body["metadata"] == payment_payload["metadata"]
    assert body["status"] == "pending"
    assert body["processed_at"] is None
    assert body["webhook_delivered_at"] is None


async def test_get_unknown_payment_returns_404(api_client: Any) -> None:
    assert (await api_client.get(f"{PATH}/{uuid.uuid4()}")).status_code == 404


async def test_get_rejects_non_uuid(api_client: Any) -> None:
    assert (await api_client.get(f"{PATH}/not-a-uuid")).status_code == 422


async def test_get_requires_api_key(api_client: Any) -> None:
    response = await api_client.get(f"{PATH}/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


async def test_replay_returns_same_payment_without_new_rows(
    api_client: Any, session_factory: Any, payment_payload: dict[str, Any]
) -> None:
    headers = {"Idempotency-Key": "replay-key"}
    first = await api_client.post(PATH, json=payment_payload, headers=headers)
    second = await api_client.post(PATH, json=payment_payload, headers=headers)

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxMessage) == 1


async def test_same_key_with_different_body_conflicts(
    api_client: Any, session_factory: Any, payment_payload: dict[str, Any]
) -> None:
    headers = {"Idempotency-Key": "conflict-key"}
    await api_client.post(PATH, json=payment_payload, headers=headers)

    response = await api_client.post(
        PATH, json={**payment_payload, "amount": "9999.00"}, headers=headers
    )

    assert response.status_code == 409
    assert "conflict-key" in response.json()["detail"]
    assert await _count(session_factory, Payment) == 1


async def test_concurrent_requests_with_one_key_create_single_payment(
    api_client: Any, session_factory: Any, payment_payload: dict[str, Any]
) -> None:
    """Гонка одинаковых POST разрешается через UNIQUE, а не дублем."""
    headers = {"Idempotency-Key": "race-key"}
    responses = await asyncio.gather(
        *(api_client.post(PATH, json=payment_payload, headers=headers) for _ in range(5))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxMessage) == 1


async def test_different_keys_create_different_payments(
    api_client: Any, session_factory: Any, payment_payload: dict[str, Any]
) -> None:
    first = await api_client.post(PATH, json=payment_payload, headers={"Idempotency-Key": "a"})
    second = await api_client.post(PATH, json=payment_payload, headers={"Idempotency-Key": "b"})

    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await _count(session_factory, Payment) == 2


async def test_healthz_is_open(api_client: Any) -> None:
    response = await api_client.get("/healthz", headers={"X-API-Key": "wrong"})
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:9000/webhook",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.1.2.3/webhook",
    ],
)
async def test_private_webhook_target_is_refused(
    api_client: Any, session_factory: Any, payment_payload: dict[str, Any], url: str
) -> None:
    """SSRF: платёж с адресом во внутренней сети не принимается вовсе."""
    response = await api_client.post(
        PATH, json={**payment_payload, "webhook_url": url}, headers={"Idempotency-Key": "ssrf"}
    )

    assert response.status_code == 422
    assert "not allowed" in response.json()["detail"]
    assert await _count(session_factory, Payment) == 0
    assert await _count(session_factory, OutboxMessage) == 0
