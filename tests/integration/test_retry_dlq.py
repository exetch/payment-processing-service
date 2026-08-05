"""Сквозные сценарии через настоящий consumer: happy path, ретраи, DLQ."""

import asyncio
import json
import socket
import threading
import time
import uuid
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest
import pytest_asyncio

from app.core.messaging import HEADER_ATTEMPT, HEADER_DLQ_REASON, ROUTING_KEY_PAYMENTS_NEW
from app.models import Currency, Payment, PaymentStatus
from app.repositories.uow import UnitOfWork
from app.workers.topology import dlq_queue, payments_exchange

# Порт 1 гарантированно недоступен — webhook падает мгновенно, без ожидания таймаута
UNREACHABLE_WEBHOOK = "http://127.0.0.1:1/webhook"


@pytest.fixture
def webhook_sink() -> Any:
    """Локальный приёмник webhook'ов, записывающий полученные тела."""
    received: list[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            received.append(json.loads(self.rfile.read(length)))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield {"url": f"http://127.0.0.1:{port}/webhook", "received": received}
    finally:
        server.shutdown()


@pytest_asyncio.fixture
async def running_consumer(broker: Any) -> Any:
    """Настоящий consumer со всей его обвязкой, включая парсинг сообщений FastStream."""
    from app.workers import consumer as module

    await module.startup()
    await module.broker.start()
    try:
        yield module
    finally:
        await module.broker.close()
        await module.shutdown()


async def _create_payment(session_factory: Any, webhook_url: str) -> uuid.UUID:
    async with UnitOfWork(session_factory) as uow:
        payment = uow.payments.add(
            Payment(
                amount=Decimal("42.00"),
                currency=Currency.EUR,
                description="e2e",
                metadata_=None,
                idempotency_key=f"e2e-{uuid.uuid4()}",
                webhook_url=webhook_url,
            )
        )
        await uow.flush()
        return payment.id


async def _publish_event(broker: Any, payload: dict | str) -> None:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    await broker.publish(
        body.encode(),
        exchange=payments_exchange,
        routing_key=ROUTING_KEY_PAYMENTS_NEW,
        content_type="application/json",
        persist=True,
    )


async def _wait_for(check: Any, timeout: float = 30.0, interval: float = 0.3) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(interval)
    return None


async def test_end_to_end_happy_path(
    broker: Any, session_factory: Any, running_consumer: Any, webhook_sink: dict
) -> None:
    payment_id = await _create_payment(session_factory, webhook_sink["url"])

    await _publish_event(broker, {"payment_id": str(payment_id)})

    async def processed() -> Any:
        async with UnitOfWork(session_factory) as uow:
            payment = await uow.payments.get(payment_id)
        return payment if payment and payment.status is not PaymentStatus.PENDING else None

    payment = await _wait_for(processed)
    assert payment is not None, "платёж не дошёл до терминального статуса"
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None

    async def webhook_arrived() -> list[dict]:
        return webhook_sink["received"]

    assert await _wait_for(webhook_arrived, timeout=10.0), "webhook не пришёл"
    body = webhook_sink["received"][0]
    assert body["payment_id"] == str(payment_id)
    assert body["status"] == "succeeded"
    assert body["amount"] == "42.00"
    assert body["currency"] == "EUR"


async def test_poison_message_lands_in_dlq_instead_of_vanishing(
    broker: Any, running_consumer: Any, drain: Any
) -> None:
    """Payload, отбракованный до вызова хендлера, уводит в DLQ сам брокер."""
    marker = uuid.uuid4()
    await _publish_event(broker, {"payment_id": f"not-a-uuid-{marker}"})

    messages = await _wait_for(lambda: drain(dlq_queue, timeout=1.0), timeout=20.0)

    assert messages, "ядовитое сообщение исчезло вместо DLQ"
    assert str(marker) in messages[0].body.decode()
    deaths = messages[0].headers["x-death"]
    assert deaths[0]["reason"] == "rejected"
    assert deaths[0]["queue"] == "payments.new"


async def test_broken_json_lands_in_dlq(
    broker: Any, running_consumer: Any, drain: Any
) -> None:
    await _publish_event(broker, "{not json at all")

    messages = await _wait_for(lambda: drain(dlq_queue, timeout=1.0), timeout=20.0)

    assert messages, "нераспарсенное сообщение исчезло вместо DLQ"
    assert b"not json" in messages[0].body


async def test_retry_ladder_ends_in_dlq_with_diagnostics(
    broker: Any, session_factory: Any, running_consumer: Any, settings: Any, drain: Any
) -> None:
    """Полный путь: первичная попытка + 3 ретрая с задержками, затем DLQ."""
    payment_id = await _create_payment(session_factory, UNREACHABLE_WEBHOOK)
    started = time.monotonic()

    await _publish_event(broker, {"payment_id": str(payment_id)})

    messages = await _wait_for(lambda: drain(dlq_queue, timeout=1.0), timeout=40.0)
    elapsed = time.monotonic() - started

    assert messages, "сообщение не доехало до DLQ"
    message = messages[0]
    assert json.loads(message.body)["payment_id"] == str(payment_id)
    assert message.headers[HEADER_ATTEMPT] == settings.consumer_max_retries + 1
    assert "network error" in message.headers[HEADER_DLQ_REASON]
    assert elapsed >= sum(settings.retry_delays_seconds), "задержки ретраев не выдержаны"

    async with UnitOfWork(session_factory) as uow:
        payment = await uow.payments.get(payment_id)
    assert payment.status is PaymentStatus.SUCCEEDED, "статус проставлен на первой попытке"
    assert payment.webhook_delivered_at is None, "webhook так и не доставлен"


async def test_working_queues_are_empty_after_dlq(
    broker: Any, session_factory: Any, running_consumer: Any, settings: Any, drain: Any
) -> None:
    """Сообщение существует ровно в одном месте — дублей в рабочих очередях нет."""
    from app.workers.topology import build_retry_queue, payments_queue

    payment_id = await _create_payment(session_factory, UNREACHABLE_WEBHOOK)
    await _publish_event(broker, {"payment_id": str(payment_id)})
    assert await _wait_for(lambda: drain(dlq_queue, timeout=1.0), timeout=40.0)

    await asyncio.sleep(1.0)
    assert await drain(payments_queue, timeout=0.5) == []
    for tier, delay in enumerate(settings.retry_delays_seconds, start=1):
        assert await drain(build_retry_queue(tier, delay), timeout=0.5) == []
