"""Инфраструктура интеграционных тестов.

Postgres и RabbitMQ поднимаются в контейнерах на сессию, поэтому тесты не зависят
от запущенного docker-compose и не конфликтуют с занятыми портами. Схему
накатывает Alembic — тот же путь, что и в проде.
"""

from collections.abc import AsyncIterator, Iterator
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio

API_KEY = "test-api-key"
SIGNING_SECRET = "test-signing-secret"


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as container:
        yield container.get_connection_url()


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    from testcontainers.community.rabbitmq import RabbitMqContainer

    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as container:
        params = container.get_connection_params()
        yield (
            f"amqp://{container.username}:{container.password}"
            f"@{container.get_container_host_ip()}:{params.port}/"
        )


@pytest.fixture(scope="session", autouse=True)
def _environment(postgres_url: str, rabbitmq_url: str) -> Iterator[None]:
    """Подставить адреса контейнеров и сбросить кеш настроек."""
    import os

    from app.core.config import get_settings

    os.environ["DATABASE_URL"] = postgres_url
    os.environ["RABBITMQ_URL"] = rabbitmq_url
    os.environ["API_KEY"] = API_KEY
    # Эмуляция шлюза в тестах мгновенная и предсказуемая
    os.environ["GATEWAY_MIN_DELAY_SECONDS"] = "0"
    os.environ["GATEWAY_MAX_DELAY_SECONDS"] = "0"
    os.environ["GATEWAY_SUCCESS_RATE"] = "1"
    os.environ["RETRY_BASE_DELAY_SECONDS"] = "1"
    os.environ["WEBHOOK_SIGNING_SECRET"] = SIGNING_SECRET
    # Приёмники в тестах — loopback и вымышленные домены
    os.environ["WEBHOOK_ALLOWED_HOSTS"] = "receiver.test,127.0.0.1,localhost"
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope="session", autouse=True)
def _schema(_environment: None) -> None:
    """Накатить миграции на чистую БД контейнера."""
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config("alembic.ini"), "head")


@pytest.fixture
def settings() -> Any:
    from app.core.config import get_settings

    return get_settings()


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[Any]:
    from app.core.db import build_engine

    engine = build_engine()
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(engine: Any) -> Any:
    from app.core.db import build_session_factory

    return build_session_factory(engine)


@pytest_asyncio.fixture(autouse=True)
async def _clean_tables(engine: Any) -> None:
    """Каждый тест начинается с пустых таблиц."""
    from sqlalchemy import text

    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox"))


@pytest_asyncio.fixture
async def uow_factory(session_factory: Any) -> Any:
    from app.repositories.uow import UnitOfWork

    return lambda: UnitOfWork(session_factory)


@pytest_asyncio.fixture
async def broker(settings: Any) -> AsyncIterator[Any]:
    """Подключённый брокер с объявленной топологией и пустыми очередями."""
    from app.core.broker import build_broker
    from app.workers.topology import declare_topology

    broker = build_broker(settings.rabbitmq_url)
    await broker.connect()
    await declare_topology(broker, settings)
    for queue in _all_queues(settings):
        declared = await broker.declare_queue(queue)
        await declared.purge()
    try:
        yield broker
    finally:
        await broker.close()


def _all_queues(settings: Any) -> list[Any]:
    from app.workers.topology import build_retry_queue, dlq_queue, payments_queue

    retries = [
        build_retry_queue(tier, delay)
        for tier, delay in enumerate(settings.retry_delays_seconds, start=1)
    ]
    return [payments_queue, dlq_queue, *retries]


@pytest.fixture
def drain(broker: Any) -> Any:
    """Забрать сообщения из очереди, дождавшись их появления."""
    import asyncio

    async def _drain(queue: Any, expected: int = 1, timeout: float = 5.0) -> list[Any]:
        declared = await broker.declare_queue(queue)
        messages: list[Any] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while len(messages) < expected and loop.time() < deadline:
            message = await declared.get(fail=False, timeout=1)
            if message is None:
                await asyncio.sleep(0.1)
                continue
            await message.ack()
            messages.append(message)
        return messages

    return _drain


@pytest_asyncio.fixture
async def api_client(uow_factory: Any) -> AsyncIterator[Any]:
    """HTTP-клиент поверх ASGI без lifespan: брокер и relay для API не нужны."""
    import httpx

    from app.main import create_app
    from app.services.payments import PaymentService

    app = create_app()
    app.state.payment_service = PaymentService(uow_factory)
    async with httpx.AsyncClient(
        # Starlette пробрасывает исключение дальше уже после ответа; клиента
        # интересует именно ответ, поэтому в тестах смотрим на него
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client


@pytest_asyncio.fixture
async def pending_payment(session_factory: Any) -> Any:
    """Платёж в статусе pending прямо в БД, без обращения к API."""
    from app.models import Currency, Payment
    from app.repositories.uow import UnitOfWork

    async with UnitOfWork(session_factory) as uow:
        payment = uow.payments.add(
            Payment(
                amount=Decimal("100.00"),
                currency=Currency.RUB,
                description="fixture payment",
                metadata_={"source": "fixture"},
                idempotency_key="fixture-key",
                webhook_url="http://receiver.test/webhook",
            )
        )
        await uow.flush()
        payment_id = payment.id
    async with UnitOfWork(session_factory) as uow:
        return await uow.payments.get(payment_id)
