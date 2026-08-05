"""Обработчик платежа: шлюз → статус → webhook, идемпотентность шагов."""

import json
import uuid
from typing import Any

import httpx
import pytest

from app.core.messaging import HEADER_ATTEMPT, HEADER_DLQ_REASON, HEADER_LAST_ERROR
from app.models import PaymentStatus
from app.workers.gateway import PaymentGatewayEmulator
from app.workers.processor import WEBHOOK_API_VERSION, PaymentProcessor
from app.workers.topology import dlq_queue
from app.workers.webhooks import WebhookSender


class StubMessage:
    """Минимальный двойник RabbitMessage: обработчик использует только эти поля."""

    def __init__(
        self, payment_id: uuid.UUID, attempt: int | None = None, message_id: str | None = None
    ) -> None:
        self.body = json.dumps({"payment_id": str(payment_id)}).encode()
        self.message_id = message_id or str(uuid.uuid4())
        self.headers: dict[str, Any] = {} if attempt is None else {HEADER_ATTEMPT: attempt}
        self.rejected = False

    async def reject(self) -> None:
        self.rejected = True

    async def nack(self) -> None:  # pragma: no cover — не должен вызываться
        raise AssertionError("nack не используется: сбой republish уходит в reject")


class RecordingGateway(PaymentGatewayEmulator):
    def __init__(self, status: PaymentStatus) -> None:
        super().__init__(min_delay=0, max_delay=0, success_rate=1.0)
        self._status = status
        self.calls = 0

    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        self.calls += 1
        return self._status


def _sender(handler: Any) -> WebhookSender:
    return WebhookSender(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        timeout=1.0,
        signing_secret="test-signing-secret",
        allowed_hosts=["receiver.test"],
    )


def _ok_sender(seen: list[dict]) -> WebhookSender:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200)

    return _sender(handler)


def _failing_sender() -> WebhookSender:
    return _sender(lambda request: httpx.Response(500))


def _processor(
    session_factory: Any, broker: Any, settings: Any, gateway: Any, sender: WebhookSender
) -> PaymentProcessor:
    return PaymentProcessor(
        session_factory=session_factory,
        gateway=gateway,
        webhook_sender=sender,
        broker=broker,
        settings=settings,
    )


async def _reload(session_factory: Any, payment_id: uuid.UUID) -> Any:
    from app.repositories.uow import UnitOfWork

    async with UnitOfWork(session_factory) as uow:
        return await uow.payments.get(payment_id)


@pytest.mark.parametrize(
    "outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED], ids=["succeeded", "failed"]
)
async def test_payment_reaches_terminal_status_and_webhook_is_sent(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any, outcome: PaymentStatus
) -> None:
    """Отказ шлюза — тоже бизнес-результат: статус терминальный, webhook уходит."""
    seen: list[dict] = []
    gateway = RecordingGateway(outcome)
    processor = _processor(session_factory, broker, settings, gateway, _ok_sender(seen))

    await processor.process(pending_payment.id, StubMessage(pending_payment.id))

    payment = await _reload(session_factory, pending_payment.id)
    assert payment.status is outcome
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert len(seen) == 1
    assert seen[0]["status"] == outcome.value
    assert seen[0]["payment_id"] == str(pending_payment.id)
    assert seen[0]["amount"] == "100.00"
    assert seen[0]["metadata"] == {"source": "fixture"}


async def test_redelivery_does_not_recharge_or_resend(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any
) -> None:
    """Повторная доставка того же сообщения не меняет ничего."""
    seen: list[dict] = []
    gateway = RecordingGateway(PaymentStatus.SUCCEEDED)
    processor = _processor(session_factory, broker, settings, gateway, _ok_sender(seen))

    await processor.process(pending_payment.id, StubMessage(pending_payment.id))
    first = await _reload(session_factory, pending_payment.id)
    await processor.process(pending_payment.id, StubMessage(pending_payment.id))
    second = await _reload(session_factory, pending_payment.id)

    assert gateway.calls == 1, "шлюз не должен вызываться повторно"
    assert len(seen) == 1, "webhook не должен дублироваться"
    assert second.processed_at == first.processed_at
    assert second.webhook_delivered_at == first.webhook_delivered_at


async def test_terminal_payment_with_undelivered_webhook_is_retried_once(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any
) -> None:
    """Если статус уже проставлен, а webhook не ушёл — повтор досылает только webhook."""
    seen: list[dict] = []
    failing = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _failing_sender(),
    )
    await failing.process(pending_payment.id, StubMessage(pending_payment.id))

    after_failure = await _reload(session_factory, pending_payment.id)
    assert after_failure.status is PaymentStatus.SUCCEEDED
    assert after_failure.webhook_delivered_at is None

    gateway = RecordingGateway(PaymentStatus.FAILED)
    healthy = _processor(session_factory, broker, settings, gateway, _ok_sender(seen))
    await healthy.process(pending_payment.id, StubMessage(pending_payment.id))

    payment = await _reload(session_factory, pending_payment.id)
    assert gateway.calls == 0, "статус уже терминальный — шлюз не трогаем"
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is not None
    assert len(seen) == 1


async def test_unknown_payment_goes_straight_to_dlq(
    session_factory: Any, broker: Any, settings: Any, drain: Any
) -> None:
    missing = uuid.uuid4()
    processor = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _ok_sender([]),
    )

    await processor.process(missing, StubMessage(missing))

    messages = await drain(dlq_queue)
    assert len(messages) == 1
    assert json.loads(messages[0].body)["payment_id"] == str(missing)
    assert "not found" in messages[0].headers[HEADER_DLQ_REASON]


async def test_webhook_failure_schedules_retry_tier(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any, drain: Any
) -> None:
    processor = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _failing_sender(),
    )

    await processor.process(pending_payment.id, StubMessage(pending_payment.id))

    messages = await drain(_retry_queue(settings, 1), timeout=0.8)
    assert len(messages) == 1
    assert messages[0].headers[HEADER_ATTEMPT] == 2
    assert "500" in messages[0].headers[HEADER_LAST_ERROR]
    payment = await _reload(session_factory, pending_payment.id)
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None


@pytest.mark.parametrize("attempt", [2, 3])
async def test_middle_attempts_go_to_matching_tier(
    session_factory: Any,
    broker: Any,
    settings: Any,
    pending_payment: Any,
    drain: Any,
    attempt: int,
) -> None:
    processor = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _failing_sender(),
    )

    await processor.process(pending_payment.id, StubMessage(pending_payment.id, attempt=attempt))

    messages = await drain(_retry_queue(settings, attempt), timeout=0.8)
    assert len(messages) == 1
    assert messages[0].headers[HEADER_ATTEMPT] == attempt + 1


async def test_exhausted_attempts_go_to_dlq(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any, drain: Any
) -> None:
    last = settings.consumer_max_retries + 1
    processor = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _failing_sender(),
    )

    await processor.process(pending_payment.id, StubMessage(pending_payment.id, attempt=last))

    messages = await drain(dlq_queue)
    assert len(messages) == 1
    assert messages[0].headers[HEADER_ATTEMPT] == last
    assert "500" in messages[0].headers[HEADER_DLQ_REASON]
    assert json.loads(messages[0].body)["payment_id"] == str(pending_payment.id)


async def test_republish_failure_rejects_instead_of_requeue(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any
) -> None:
    """Сбой самого republish не должен возвращать сообщение в очередь с прежним x-attempt."""

    class BrokenBroker:
        async def publish(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("broker is down")

    processor = _processor(
        session_factory,
        BrokenBroker(),
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _failing_sender(),
    )
    message = StubMessage(pending_payment.id)

    await processor.process(pending_payment.id, message)

    assert message.rejected is True, "сообщение должно уйти в DLQ через reject брокера"


def _retry_queue(settings: Any, tier: int) -> Any:
    from app.workers.topology import build_retry_queue

    return build_retry_queue(tier, settings.retry_delays_seconds[tier - 1])


async def test_webhook_payload_carries_event_id_and_version(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any
) -> None:
    seen: list[dict] = []
    processor = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _ok_sender(seen),
    )

    await processor.process(
        pending_payment.id, StubMessage(pending_payment.id, message_id="evt-42")
    )

    assert seen[0]["event_id"] == "evt-42"
    assert seen[0]["event"] == "payment.processed"
    assert seen[0]["api_version"] == WEBHOOK_API_VERSION


async def test_event_id_is_stable_across_retries(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any
) -> None:
    """Иначе получателю нечем дедуплицировать повторную доставку одного события."""
    seen: list[dict] = []
    failing = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _failing_sender(),
    )
    await failing.process(pending_payment.id, StubMessage(pending_payment.id, message_id="evt-7"))

    healthy = _processor(
        session_factory,
        broker,
        settings,
        RecordingGateway(PaymentStatus.SUCCEEDED),
        _ok_sender(seen),
    )
    await healthy.process(
        pending_payment.id, StubMessage(pending_payment.id, attempt=2, message_id="evt-7")
    )

    assert seen[0]["event_id"] == "evt-7"


async def test_forbidden_webhook_target_goes_straight_to_dlq(
    session_factory: Any, broker: Any, settings: Any, pending_payment: Any, drain: Any
) -> None:
    """Запрещённый адрес — постоянный отказ: ретраить нечего."""
    strict_sender = WebhookSender(
        httpx.AsyncClient(),
        timeout=1.0,
        signing_secret="test-signing-secret",
    )
    processor = _processor(
        session_factory, broker, settings, RecordingGateway(PaymentStatus.SUCCEEDED), strict_sender
    )

    await processor.process(pending_payment.id, StubMessage(pending_payment.id))

    messages = await drain(dlq_queue)
    assert len(messages) == 1, "должно уйти в DLQ, минуя retry-тиры"
    assert "not resolvable" in messages[0].headers[HEADER_DLQ_REASON]
    assert await drain(_retry_queue(settings, 1), timeout=0.5) == []
