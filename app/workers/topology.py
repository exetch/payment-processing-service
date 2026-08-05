"""Объявление RabbitMQ-топологии.

Схема:

    exchange payments        (direct) ──payments.new───────→ queue payments.new
                                                              (DLX → payments.dlx)
    exchange payments.retry  (direct) ──<имя очереди>──────→ payments.new.retry.{1..N}
                                         (TTL 2s/4s/8s; DLX → payments / payments.new)
    exchange payments.dlx    (direct) ──payments.new───────→ queue payments.new.dlq

Retry-очереди без consumer'ов: сообщение отлёживается TTL очереди и dead-letter'ится
обратно в payments.new. TTL фиксированный на очередь: per-message expiration
подвержен head-of-line blocking.

У payments.new тоже есть DLX, это страховка от потери сообщений, которые
отвергнуты мимо кода обработчика: FastStream reject'ит сообщение с невалидным
payload (не UUID в payment_id, битый JSON) до вызова хендлера, и без DLX брокер
удалял бы такое сообщение навсегда.
"""

from aio_pika.exceptions import ChannelPreconditionFailed
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import Settings
from app.core.messaging import (
    EXCHANGE_DLX,
    EXCHANGE_PAYMENTS,
    EXCHANGE_RETRY,
    QUEUE_PAYMENTS_DLQ,
    QUEUE_PAYMENTS_NEW,
    ROUTING_KEY_PAYMENTS_NEW,
    retry_queue_name,
)

payments_exchange = RabbitExchange(EXCHANGE_PAYMENTS, type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange(EXCHANGE_RETRY, type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange(EXCHANGE_DLX, type=ExchangeType.DIRECT, durable=True)

payments_queue = RabbitQueue(
    QUEUE_PAYMENTS_NEW,
    durable=True,
    routing_key=ROUTING_KEY_PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": EXCHANGE_DLX,
        "x-dead-letter-routing-key": ROUTING_KEY_PAYMENTS_NEW,
    },
)
dlq_queue = RabbitQueue(QUEUE_PAYMENTS_DLQ, durable=True, routing_key=ROUTING_KEY_PAYMENTS_NEW)


def build_retry_queue(tier: int, delay_seconds: float) -> RabbitQueue:
    name = retry_queue_name(tier)
    return RabbitQueue(
        name,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": int(delay_seconds * 1000),
            "x-dead-letter-exchange": EXCHANGE_PAYMENTS,
            "x-dead-letter-routing-key": ROUTING_KEY_PAYMENTS_NEW,
        },
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Идемпотентно объявить обменники, очереди и байндинги; повтор это no-op."""
    try:
        ex_payments = await broker.declare_exchange(payments_exchange)
        ex_retry = await broker.declare_exchange(retry_exchange)
        ex_dlx = await broker.declare_exchange(dlx_exchange)

        q_main = await broker.declare_queue(payments_queue)
        await q_main.bind(ex_payments, routing_key=ROUTING_KEY_PAYMENTS_NEW)

        for tier, delay in enumerate(settings.retry_delays_seconds, start=1):
            q_retry = await broker.declare_queue(build_retry_queue(tier, delay))
            await q_retry.bind(ex_retry, routing_key=retry_queue_name(tier))

        q_dlq = await broker.declare_queue(dlq_queue)
        await q_dlq.bind(ex_dlx, routing_key=ROUTING_KEY_PAYMENTS_NEW)
    except ChannelPreconditionFailed as exc:
        raise RuntimeError(
            "Очередь уже существует с другими аргументами (RabbitMQ запрещает менять их "
            "на лету). Удалите очереди payments.*, например пересоздав контейнер "
            "брокера: docker compose up -d --force-recreate rabbitmq"
        ) from exc
