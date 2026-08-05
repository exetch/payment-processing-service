"""Unit of Work: границы транзакции и доступ к репозиториям.

Единственная точка commit/rollback; сервисы и workers ходят в БД только через UoW.
"""

from types import TracebackType
from typing import Self

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import DuplicateIdempotencyKeyError
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository

_IDEMPOTENCY_CONSTRAINT = "uq_payments_idempotency_key"


class UnitOfWork:
    payments: PaymentRepository
    outbox: OutboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self.payments = PaymentRepository(self._session)
        self.outbox = OutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None:
                await self._session.commit()
            else:
                await self._session.rollback()
        finally:
            await self._session.close()

    async def flush(self) -> None:
        """Flush с переводом нарушения UNIQUE(idempotency_key) в доменную ошибку."""
        try:
            await self._session.flush()
        except IntegrityError as exc:
            if _IDEMPOTENCY_CONSTRAINT in str(exc.orig):
                raise DuplicateIdempotencyKeyError(str(exc.orig)) from exc
            raise
