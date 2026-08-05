"""Пробы состояния: /healthz — liveness, /readyz — реальная готовность."""

import asyncio
from typing import Any

import httpx
import pytest_asyncio

from app.core.broker import build_broker
from app.main import create_app
from app.services.payments import PaymentService
from app.workers.outbox_relay import OutboxRelay


class DeadEngine:
    def connect(self) -> Any:
        raise OSError("database is gone")


class DeadBroker:
    async def ping(self, timeout: float) -> bool:
        return False


@pytest_asyncio.fixture
async def probe_client(
    engine: Any, session_factory: Any, settings: Any, uow_factory: Any
) -> Any:
    """Приложение с настоящими зависимостями и живой relay-задачей."""
    broker = build_broker(settings.rabbitmq_url)
    await broker.connect()
    relay = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)
    task = asyncio.create_task(relay.run())

    app = create_app()
    app.state.payment_service = PaymentService(uow_factory)
    app.state.engine = engine
    app.state.broker = broker
    app.state.relay = relay
    app.state.relay_task = task

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        try:
            yield client, app
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await broker.close()


async def test_ready_when_everything_works(probe_client: Any) -> None:
    client, _ = probe_client
    response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {"database": "ok", "broker": "ok", "outbox_relay": "ok"},
    }


async def test_not_ready_when_database_is_down(probe_client: Any) -> None:
    client, app = probe_client
    app.state.engine = DeadEngine()

    response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "not ready"
    assert response.json()["checks"]["database"].startswith("error: OSError")


async def test_not_ready_when_broker_is_unreachable(probe_client: Any) -> None:
    client, app = probe_client
    app.state.broker = DeadBroker()

    response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["broker"] == "unreachable"


async def test_not_ready_when_relay_died(probe_client: Any) -> None:
    """Смерть relay обязана краснить готовность: события перестают публиковаться."""
    client, app = probe_client
    app.state.relay_task.cancel()
    await asyncio.gather(app.state.relay_task, return_exceptions=True)

    response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["checks"]["outbox_relay"] == "dead"


async def test_not_ready_when_relay_keeps_failing(
    probe_client: Any, broker: Any, session_factory: Any, settings: Any
) -> None:
    """Работающий, но постоянно сбоящий relay — тоже причина не пускать трафик."""
    client, app = probe_client
    # Подменяем на незапущенный экземпляр: у живого счётчик сбрасывается каждой
    # успешной итерацией, и подмена не пережила бы следующий опрос outbox.
    stalled = OutboxRelay(broker=broker, session_factory=session_factory, settings=settings)
    stalled._consecutive_failures = 99
    app.state.relay = stalled

    response = await client.get("/readyz")

    assert response.status_code == 503
    assert "failing" in response.json()["checks"]["outbox_relay"]


async def test_healthz_stays_green_when_dependencies_are_down(probe_client: Any) -> None:
    """Liveness не должен ронять контейнер из-за недоступной БД."""
    client, app = probe_client
    app.state.engine = DeadEngine()
    app.state.broker = DeadBroker()

    assert (await client.get("/healthz")).status_code == 200
