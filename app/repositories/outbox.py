import uuid
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, message: OutboxMessage) -> OutboxMessage:
        self._session.add(message)
        return message

    async def claim_next(self, now: datetime) -> OutboxMessage | None:
        """Захватить одно неопубликованное событие, у которого подошло время.

        FOR UPDATE SKIP LOCKED: параллельные relay берут разные строки. Порядок по
        next_attempt_at, а не только по created_at, поэтому отложенная после сбоя
        строка не мешает остальным.
        """
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None), OutboxMessage.next_attempt_at <= now)
            .order_by(OutboxMessage.next_attempt_at, OutboxMessage.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def mark_published(self, message_id: uuid.UUID, published_at: datetime) -> None:
        stmt = (
            update(OutboxMessage)
            .where(OutboxMessage.id == message_id)
            .values(published_at=published_at)
        )
        await self._session.execute(stmt)

    async def postpone(
        self, message_id: uuid.UUID, attempts: int, error: str, next_attempt_at: datetime
    ) -> None:
        """Отложить неудачную публикацию, сохранив причину."""
        stmt = (
            update(OutboxMessage)
            .where(OutboxMessage.id == message_id)
            .values(attempts=attempts, last_error=error[:500], next_attempt_at=next_attempt_at)
        )
        await self._session.execute(stmt)
