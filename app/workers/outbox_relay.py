"""Outbox relay: доставка событий из таблицы outbox в RabbitMQ.

Каждое событие публикуется и помечается в собственной транзакции. Сбой на одном
сообщении не откатывает уже опубликованные и не блокирует остальные: строка
откладывается с экспоненциальной задержкой, а relay идёт дальше.

At-least-once: ``published_at`` коммитится только после успешной публикации,
дубликаты гасит идемпотентный consumer.
"""

import asyncio
import enum
import logging
import uuid
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.repositories.uow import UnitOfWork
from app.workers.topology import payments_exchange

logger = logging.getLogger(__name__)


class _Outcome(enum.StrEnum):
    PUBLISHED = "published"
    POSTPONED = "postponed"
    EMPTY = "empty"


class OutboxRelay:
    def __init__(
        self,
        broker: RabbitBroker,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._broker = broker
        self._session_factory = session_factory
        self._poll_interval = settings.outbox_poll_interval_seconds
        self._batch_size = settings.outbox_batch_size
        self._retry_base = settings.outbox_retry_base_delay_seconds
        self._retry_max = settings.outbox_retry_max_delay_seconds
        self._consecutive_failures = 0
        self._last_success_at: datetime | None = None

    @property
    def consecutive_failures(self) -> int:
        """Подряд идущие неудачи; сбрасывается только успешной публикацией."""
        return self._consecutive_failures

    @property
    def last_success_at(self) -> datetime | None:
        return self._last_success_at

    async def run(self) -> None:
        logger.info("outbox relay started")
        try:
            while True:
                try:
                    published = await self._drain_once()
                except Exception:
                    # Сюда доходят только сбои самой БД: ошибки публикации
                    # обрабатываются по одному сообщению внутри _publish_next
                    self._register_failure()
                    logger.exception(
                        "outbox relay iteration failed",
                        extra={"ctx": {"consecutive_failures": self._consecutive_failures}},
                    )
                    published = 0
                if published == 0:
                    await asyncio.sleep(self._poll_interval)
        finally:
            logger.info("outbox relay stopped")

    async def _drain_once(self) -> int:
        published = 0
        for _ in range(self._batch_size):
            outcome = await self._publish_next()
            if outcome is _Outcome.EMPTY:
                break
            if outcome is _Outcome.PUBLISHED:
                published += 1
        if published:
            logger.info("outbox drained", extra={"ctx": {"published": published}})
        return published

    async def _publish_next(self) -> _Outcome:
        async with UnitOfWork(self._session_factory) as uow:
            message = await uow.outbox.claim_next(datetime.now(tz=UTC))
            if message is None:
                return _Outcome.EMPTY
            message_id, attempts = message.id, message.attempts
            try:
                await self._broker.publish(
                    message.payload,
                    exchange=payments_exchange,
                    routing_key=message.routing_key,
                    message_id=str(message_id),
                    persist=True,
                )
            except Exception as exc:
                # Не поднимаем наружу: перенос попытки нужно закоммитить, иначе
                # строка будет выбираться снова на полной скорости поллинга
                await self._postpone(uow, message_id, attempts + 1, exc)
                return _Outcome.POSTPONED
            await uow.outbox.mark_published(message_id, datetime.now(tz=UTC))
        self._register_success()
        return _Outcome.PUBLISHED

    async def _postpone(
        self, uow: UnitOfWork, message_id: uuid.UUID, attempts: int, exc: Exception
    ) -> None:
        delay = min(self._retry_base * 2 ** (attempts - 1), self._retry_max)
        await uow.outbox.postpone(
            message_id,
            attempts=attempts,
            error=f"{type(exc).__name__}: {exc}",
            next_attempt_at=datetime.now(tz=UTC) + timedelta(seconds=delay),
        )
        self._register_failure()
        logger.exception(
            "outbox publish failed, postponed",
            extra={
                "ctx": {
                    "outbox_id": str(message_id),
                    "attempts": attempts,
                    "delay_seconds": delay,
                }
            },
        )

    def _register_success(self) -> None:
        self._consecutive_failures = 0
        self._last_success_at = datetime.now(tz=UTC)

    def _register_failure(self) -> None:
        self._consecutive_failures += 1
