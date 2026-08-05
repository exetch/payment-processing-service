"""Бизнес-логика платежей."""

import logging
import uuid
from collections.abc import Callable, Collection
from datetime import UTC, datetime

from app.core.exceptions import DuplicateIdempotencyKeyError, IdempotencyConflictError
from app.core.messaging import ROUTING_KEY_PAYMENTS_NEW
from app.core.net import check_url_literal
from app.models import OutboxMessage, Payment
from app.repositories.uow import UnitOfWork
from app.schemas import PaymentCreate, PaymentOut

logger = logging.getLogger(__name__)

UowFactory = Callable[[], UnitOfWork]


class PaymentService:
    def __init__(self, uow_factory: UowFactory, allowed_hosts: Collection[str] = ()) -> None:
        self._uow_factory = uow_factory
        self._allowed_hosts = allowed_hosts

    async def create_payment(
        self, data: PaymentCreate, idempotency_key: str
    ) -> tuple[PaymentOut, bool]:
        """Создать платёж и outbox-событие в одной транзакции.

        Возвращает ``(payment, created)``. Повтор с тем же ключом и тем же телом это
        ``(существующий платёж, False)`` без новых записей; тот же ключ с другим
        телом даёт ``IdempotencyConflictError`` (409).
        """
        # Отказываем сразу: принимать платёж, о котором заведомо нельзя уведомить,
        # хуже, чем вернуть ошибку синхронно
        check_url_literal(str(data.webhook_url), self._allowed_hosts)
        try:
            async with self._uow_factory() as uow:
                existing = await uow.payments.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    return self._duplicate_response(existing, data, idempotency_key)

                payment = uow.payments.add(
                    Payment(
                        amount=data.amount,
                        currency=data.currency,
                        description=data.description,
                        metadata_=data.metadata,
                        idempotency_key=idempotency_key,
                        webhook_url=str(data.webhook_url),
                    )
                )
                await uow.flush()
                uow.outbox.add(
                    OutboxMessage(
                        routing_key=ROUTING_KEY_PAYMENTS_NEW,
                        payload={
                            "payment_id": str(payment.id),
                            "occurred_at": datetime.now(tz=UTC).isoformat(),
                        },
                    )
                )
        except DuplicateIdempotencyKeyError:
            return await self._resolve_race(data, idempotency_key)

        logger.info(
            "payment created",
            extra={"ctx": {"payment_id": str(payment.id), "idempotency_key": idempotency_key}},
        )
        return PaymentOut.from_model(payment), True

    async def get_payment(self, payment_id: uuid.UUID) -> PaymentOut | None:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
        return None if payment is None else PaymentOut.from_model(payment)

    async def _resolve_race(
        self, data: PaymentCreate, idempotency_key: str
    ) -> tuple[PaymentOut, bool]:
        """Конкурент успел вставить платёж с этим ключом, отвечаем как на повтор."""
        async with self._uow_factory() as uow:
            existing = await uow.payments.get_by_idempotency_key(idempotency_key)
        if existing is None:
            raise RuntimeError(
                f"payment with idempotency key {idempotency_key!r} vanished after conflict"
            )
        return self._duplicate_response(existing, data, idempotency_key)

    def _duplicate_response(
        self, existing: Payment, data: PaymentCreate, idempotency_key: str
    ) -> tuple[PaymentOut, bool]:
        if not self._payloads_match(existing, data):
            logger.warning(
                "idempotency conflict",
                extra={
                    "ctx": {
                        "payment_id": str(existing.id),
                        "idempotency_key": idempotency_key,
                    }
                },
            )
            raise IdempotencyConflictError(idempotency_key)
        logger.info(
            "duplicate payment request",
            extra={"ctx": {"payment_id": str(existing.id), "idempotency_key": idempotency_key}},
        )
        return PaymentOut.from_model(existing), False

    @staticmethod
    def _payloads_match(existing: Payment, data: PaymentCreate) -> bool:
        return (
            existing.amount == data.amount
            and existing.currency == data.currency
            and existing.description == data.description
            and existing.metadata_ == data.metadata
            and existing.webhook_url == str(data.webhook_url)
        )
