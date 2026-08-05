import uuid
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, payment: Payment) -> Payment:
        self._session.add(payment)
        return payment

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == key)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def finalize_if_pending(
        self, payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> bool:
        """Guard-update: перевести pending-платёж в терминальный статус.

        True значит, что статус изменила эта операция; при гонке/редоставке побеждает ровно один.
        """
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at)
            .returning(Payment.id)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def mark_webhook_delivered(self, payment_id: uuid.UUID, delivered_at: datetime) -> None:
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=delivered_at)
        )
        await self._session.execute(stmt)
