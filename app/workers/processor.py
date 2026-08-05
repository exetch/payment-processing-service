"""Обработка события платежа: шлюз → статус в БД → webhook; ретраи и DLQ.

Шаги идемпотентны по состоянию в БД: редоставка сообщения не меняет уже
терминальный статус и не шлёт webhook повторно. Обращение к шлюзу выполняется
до фиксации статуса, поэтому падение ровно в этом окне даёт повторный вызов:
реальному гейтвею сюда нужно передавать idempotency-key.
"""

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitMessage
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.messaging import (
    HEADER_ATTEMPT,
    HEADER_DLQ_REASON,
    HEADER_LAST_ERROR,
    ROUTING_KEY_PAYMENTS_NEW,
    retry_queue_name,
)
from app.core.net import UnsafeWebhookTargetError
from app.models import Payment, PaymentStatus
from app.repositories.uow import UnitOfWork
from app.workers.gateway import PaymentGatewayEmulator
from app.workers.topology import dlx_exchange, retry_exchange
from app.workers.webhooks import WebhookSender

logger = logging.getLogger(__name__)

# Версия формата уведомления: получателю нужно уметь пережить его эволюцию
WEBHOOK_API_VERSION = "2026-08-05"


class _PaymentNotFoundError(RuntimeError):
    """payment_id из сообщения отсутствует в БД, ретраи бессмысленны, сразу DLQ."""


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGatewayEmulator,
        webhook_sender: WebhookSender,
        broker: RabbitBroker,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._broker = broker
        self._settings = settings

    async def process(self, payment_id: uuid.UUID, msg: RabbitMessage) -> None:
        """Точка входа обработчика; исключения наружу не поднимает.

        Сбой шага → republish в retry-тир или DLQ; сбой самого republish → reject,
        и сообщение уводит в DLQ уже брокер (у payments.new объявлен DLX).
        """
        attempt = self._attempt_number(msg)
        log_ctx: dict[str, Any] = {"payment_id": str(payment_id), "attempt": attempt}
        event_id = msg.message_id or str(payment_id)
        try:
            await self._handle(payment_id, event_id, log_ctx)
        except (_PaymentNotFoundError, UnsafeWebhookTargetError) as exc:
            # Постоянные отказы: ни платёж, ни адрес назначения от повтора не исправятся
            logger.error(
                "permanent failure, dead-lettering",
                extra={"ctx": {**log_ctx, "reason": str(exc)}},
            )
            await self._quarantine(msg, attempt, reason=str(exc), log_ctx=log_ctx)
        except Exception as exc:
            logger.exception("payment processing failed", extra={"ctx": log_ctx})
            await self._retry_or_quarantine(msg, attempt, exc, log_ctx)

    async def _handle(
        self, payment_id: uuid.UUID, event_id: str, log_ctx: dict[str, Any]
    ) -> None:
        payment = await self._load(payment_id)
        if payment is None:
            raise _PaymentNotFoundError(f"payment {payment_id} not found")

        if payment.status is PaymentStatus.PENDING:
            # вызов шлюза (секунды) идёт вне транзакции БД
            result = await self._gateway.charge(payment_id)
            processed_at = datetime.now(tz=UTC)
            async with UnitOfWork(self._session_factory) as uow:
                won = await uow.payments.finalize_if_pending(payment_id, result, processed_at)
            if won:
                payment.status = result
                payment.processed_at = processed_at
            else:
                logger.info("payment already finalized elsewhere", extra={"ctx": log_ctx})
                payment = await self._load(payment_id)
                if payment is None:
                    raise _PaymentNotFoundError(f"payment {payment_id} vanished")

        if payment.webhook_delivered_at is None:
            await self._webhook_sender.send(
                payment.webhook_url, self._webhook_payload(payment, event_id)
            )
            async with UnitOfWork(self._session_factory) as uow:
                await uow.payments.mark_webhook_delivered(payment.id, datetime.now(tz=UTC))
        else:
            logger.info("webhook already delivered, skipping", extra={"ctx": log_ctx})

        logger.info(
            "payment processed",
            extra={"ctx": {**log_ctx, "status": payment.status.value}},
        )

    async def _load(self, payment_id: uuid.UUID) -> Payment | None:
        async with UnitOfWork(self._session_factory) as uow:
            return await uow.payments.get(payment_id)

    def _webhook_payload(self, payment: Payment, event_id: str) -> dict[str, Any]:
        return {
            # event_id стабилен по всей цепочке ретраев (это id outbox-события),
            # поэтому получателю есть по чему дедуплицировать повторную доставку
            "event_id": event_id,
            "event": "payment.processed",
            "api_version": WEBHOOK_API_VERSION,
            "payment_id": str(payment.id),
            "status": payment.status.value,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "description": payment.description,
            "metadata": payment.metadata_,
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }

    def _attempt_number(self, msg: RabbitMessage) -> int:
        raw = (msg.headers or {}).get(HEADER_ATTEMPT, 1)
        try:
            return max(1, int(raw))
        except (TypeError, ValueError):
            return 1

    async def _retry_or_quarantine(
        self, msg: RabbitMessage, attempt: int, exc: Exception, log_ctx: dict[str, Any]
    ) -> None:
        error_text = f"{type(exc).__name__}: {exc}"[:500]
        if attempt <= self._settings.consumer_max_retries:
            delay = self._settings.retry_delays_seconds[attempt - 1]
            await self._republish(
                msg,
                exchange=retry_exchange,
                routing_key=retry_queue_name(attempt),
                headers={HEADER_ATTEMPT: attempt + 1, HEADER_LAST_ERROR: error_text},
                log_message="retry scheduled",
                log_ctx={**log_ctx, "retry_tier": attempt, "delay_seconds": delay},
            )
        else:
            await self._quarantine(msg, attempt, reason=error_text, log_ctx=log_ctx)

    async def _quarantine(
        self, msg: RabbitMessage, attempt: int, reason: str, log_ctx: dict[str, Any]
    ) -> None:
        await self._republish(
            msg,
            exchange=dlx_exchange,
            routing_key=ROUTING_KEY_PAYMENTS_NEW,
            headers={HEADER_ATTEMPT: attempt, HEADER_DLQ_REASON: reason},
            log_message="message dead-lettered",
            log_ctx={**log_ctx, "reason": reason},
        )

    async def _republish(
        self,
        msg: RabbitMessage,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        headers: dict[str, Any],
        log_message: str,
        log_ctx: dict[str, Any],
    ) -> None:
        """Переслать исходное тело сообщения; при сбое publish уходит reject в DLQ."""
        try:
            await self._broker.publish(
                msg.body,
                exchange=exchange,
                routing_key=routing_key,
                headers={**(msg.headers or {}), **headers},
                message_id=msg.message_id,
                content_type="application/json",
                persist=True,
            )
        except Exception:
            logger.exception("republish failed, rejecting to DLQ", extra={"ctx": log_ctx})
            await self._reject(msg, log_ctx)
            return
        logger.warning(log_message, extra={"ctx": log_ctx})

    async def _reject(self, msg: RabbitMessage, log_ctx: dict[str, Any]) -> None:
        """Отдать сообщение брокеру: reject(requeue=False) → DLX очереди payments.new.

        Requeue здесь не годится: сообщение вернулось бы в payments.new мгновенно и с
        прежним x-attempt, а если republish падает стабильно (снят байндинг retry-обменника),
        это бесконечный цикл вместо видимой в DLQ проблемы. Сбой самого reject означает
        мёртвый канал: сообщение останется unacked и будет редоставлено после
        переподключения, поэтому здесь достаточно журналирования.
        """
        try:
            await msg.reject()
        except Exception:
            logger.exception(
                "reject failed; message left unacked for redelivery", extra={"ctx": log_ctx}
            )
