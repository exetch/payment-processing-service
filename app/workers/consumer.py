"""Consumer платежей: FastStream-приложение.

Один обработчик: эмуляция шлюза, обновление статуса, webhook-уведомление;
ошибки уходят в retry-тиры или DLQ (см. processor.py).

Запуск: ``faststream run app.workers.consumer:app``
"""

import logging
import uuid
from datetime import datetime
from typing import Any

import httpx
from faststream import FastStream
from faststream.broker.message import decode_message
from faststream.rabbit import RabbitMessage
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.broker import build_broker
from app.core.config import get_settings
from app.core.db import build_engine, build_session_factory
from app.core.logging import setup_logging
from app.workers.gateway import PaymentGatewayEmulator
from app.workers.processor import PaymentProcessor
from app.workers.topology import declare_topology, payments_exchange, payments_queue
from app.workers.webhooks import WebhookSender

logger = logging.getLogger(__name__)

settings = get_settings()
broker = build_broker(settings.rabbitmq_url)
app = FastStream(broker)

_engine: AsyncEngine | None = None
_http_client: httpx.AsyncClient | None = None
_processor: PaymentProcessor | None = None


class PaymentEvent(BaseModel):
    """Payload события payments.new."""

    payment_id: uuid.UUID
    occurred_at: datetime | None = None


@app.on_startup
async def startup() -> None:
    global _engine, _http_client, _processor
    setup_logging(settings.log_level)
    _engine = build_engine()
    _http_client = httpx.AsyncClient()
    _processor = PaymentProcessor(
        session_factory=build_session_factory(_engine),
        gateway=PaymentGatewayEmulator(
            min_delay=settings.gateway_min_delay_seconds,
            max_delay=settings.gateway_max_delay_seconds,
            success_rate=settings.gateway_success_rate,
        ),
        webhook_sender=WebhookSender(
            _http_client,
            timeout=settings.webhook_timeout_seconds,
            signing_secret=settings.webhook_signing_secret,
            allowed_hosts=settings.webhook_allowed_host_set,
        ),
        broker=broker,
        settings=settings,
    )
    await broker.connect()
    await declare_topology(broker, settings)
    logger.info("consumer started")


@app.on_shutdown
async def shutdown() -> None:
    if _http_client is not None:
        await _http_client.aclose()
    if _engine is not None:
        await _engine.dispose()
    logger.info("consumer stopped")


async def tolerant_decoder(msg: RabbitMessage) -> Any:
    """Не дать ошибке декодирования утечь мимо подтверждения сообщения.

    FastStream декодирует тело до входа в acknowledgement-скоуп, поэтому
    исключение здесь оставило бы сообщение навсегда unacked: ни ack, ни reject,
    ни DLQ. Возвращаем заглушку: её отбракует уже валидация внутри скоупа,
    и сообщение штатно уедет в payments.new.dlq.
    """
    try:
        return decode_message(msg)
    except Exception:
        logger.warning(
            "undecodable message body",
            extra={"ctx": {"message_id": msg.message_id, "size": len(msg.body)}},
        )
        return {"undecodable": msg.body[:200].decode("utf-8", errors="replace")}


@broker.subscriber(payments_queue, payments_exchange, decoder=tolerant_decoder)
async def handle_payment(event: PaymentEvent, msg: RabbitMessage) -> None:
    if _processor is None:
        raise RuntimeError("processor is not initialized")
    await _processor.process(event.payment_id, msg)
