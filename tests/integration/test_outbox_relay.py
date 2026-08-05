"""Relay: доставка событий из outbox в брокер и поведение при сбоях."""

import asyncio
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from app.models import OutboxMessage
from app.workers.outbox_relay import OutboxRelay
from app.workers.topology import payments_queue

ROUTABLE = "payments.new"
UNROUTABLE = "no.such.binding"


async def _add_outbox_row(session_factory: Any, routing_key: str, payload: dict) -> Any:
    from app.repositories.uow import UnitOfWork

    async with UnitOfWork(session_factory) as uow:
        message = uow.outbox.add(OutboxMessage(routing_key=routing_key, payload=payload))
        await uow.flush()
        return message.id


async def _rows(session_factory: Any) -> list[OutboxMessage]:
    async with session_factory() as session:
        result = await session.execute(select(OutboxMessage).order_by(OutboxMessage.created_at))
        return list(result.scalars().all())


async def _row(session_factory: Any, row_id: Any) -> OutboxMessage:
    async with session_factory() as session:
        stmt = select(OutboxMessage).where(OutboxMessage.id == row_id)
        return (await session.execute(stmt)).scalar_one()


def _relay(broker: Any, session_factory: Any, settings: Any, **overrides: Any) -> OutboxRelay:
    return OutboxRelay(
        broker=broker,
        session_factory=session_factory,
        settings=settings.model_copy(update=overrides) if overrides else settings,
    )


async def test_publishes_pending_rows_and_marks_them(
    broker: Any, session_factory: Any, settings: Any, drain: Any
) -> None:
    await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "p-1"})
    relay = _relay(broker, session_factory, settings)

    assert await relay._drain_once() == 1

    assert (await _rows(session_factory))[0].published_at is not None
    assert len(await drain(payments_queue)) == 1


async def test_published_message_carries_payload_and_message_id(
    broker: Any, session_factory: Any, settings: Any, drain: Any
) -> None:
    import json

    row_id = await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "p-2"})
    await _relay(broker, session_factory, settings)._drain_once()

    message = (await drain(payments_queue))[0]

    assert json.loads(message.body)["payment_id"] == "p-2"
    assert message.message_id == str(row_id)
    # persistent: сообщение переживёт перезапуск брокера
    assert message.delivery_mode == 2


async def test_already_published_rows_are_not_resent(
    broker: Any, session_factory: Any, settings: Any, drain: Any
) -> None:
    await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "p-3"})
    relay = _relay(broker, session_factory, settings)

    assert await relay._drain_once() == 1
    assert await relay._drain_once() == 0

    assert len(await drain(payments_queue, expected=2, timeout=2)) == 1


async def test_unroutable_message_keeps_row_unpublished(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    """Ключевая гарантия: недоставленное событие не считается отправленным."""
    row_id = await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "p-4"})

    assert await _relay(broker, session_factory, settings)._drain_once() == 0

    row = await _row(session_factory, row_id)
    assert row.published_at is None, "unroutable-строка не должна помечаться published"
    assert row.attempts == 1
    assert row.last_error
    assert row.next_attempt_at > datetime.now(tz=UTC), "строка должна быть отложена"


async def test_failing_row_does_not_block_the_others(
    broker: Any, session_factory: Any, settings: Any, drain: Any
) -> None:
    """Голова очереди не заперта: сбойная строка откладывается, остальные едут."""
    bad = await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "bad"})
    good = await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "good"})

    published = await _relay(broker, session_factory, settings)._drain_once()

    assert published == 1
    assert (await _row(session_factory, good)).published_at is not None
    assert (await _row(session_factory, bad)).published_at is None
    assert len(await drain(payments_queue)) == 1


async def test_successful_rows_survive_a_neighbour_failure(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    """Транзакция на сообщение: сбой не откатывает уже опубликованные."""
    first = await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "first"})
    await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "boom"})
    third = await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "third"})

    assert await _relay(broker, session_factory, settings)._drain_once() == 2

    assert (await _row(session_factory, first)).published_at is not None
    assert (await _row(session_factory, third)).published_at is not None


async def test_retry_delay_grows_with_attempts(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    row_id = await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "p-5"})
    relay = _relay(
        broker,
        session_factory,
        settings,
        outbox_retry_base_delay_seconds=0.1,
        outbox_retry_max_delay_seconds=5,
    )

    await relay._drain_once()
    first = await _row(session_factory, row_id)
    await asyncio.sleep(0.25)
    await relay._drain_once()
    second = await _row(session_factory, row_id)

    assert first.attempts == 1
    assert second.attempts == 2
    assert second.next_attempt_at > first.next_attempt_at


async def test_postponed_row_is_skipped_until_due(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "p-6"})
    relay = _relay(broker, session_factory, settings, outbox_retry_base_delay_seconds=30)

    await relay._drain_once()
    before = (await _rows(session_factory))[0].attempts
    await relay._drain_once()
    after = (await _rows(session_factory))[0].attempts

    assert before == 1
    assert after == 1, "строка не должна перевыбираться до next_attempt_at"


async def test_relay_loop_survives_failures_and_reports_them(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "p-7"})
    relay = _relay(broker, session_factory, settings, outbox_retry_base_delay_seconds=0.1)

    task = asyncio.create_task(relay.run())
    await asyncio.sleep(1.0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert relay.consecutive_failures > 0
    assert relay.last_success_at is None
    assert (await _rows(session_factory))[0].published_at is None


async def test_successful_publish_resets_failure_counter(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    await _add_outbox_row(session_factory, UNROUTABLE, {"payment_id": "bad"})
    relay = _relay(broker, session_factory, settings)

    await relay._drain_once()
    assert relay.consecutive_failures == 1

    await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": "good"})
    await relay._drain_once()

    assert relay.consecutive_failures == 0
    assert relay.last_success_at is not None


async def test_batch_is_bounded_by_settings(
    broker: Any, session_factory: Any, settings: Any
) -> None:
    for index in range(5):
        await _add_outbox_row(session_factory, ROUTABLE, {"payment_id": f"batch-{index}"})
    relay = _relay(broker, session_factory, settings, outbox_batch_size=2)

    assert await relay._drain_once() == 2
    assert await relay._drain_once() == 2
    assert await relay._drain_once() == 1
    assert await relay._drain_once() == 0
