import uuid

import pytest

from app.models import PaymentStatus
from app.workers.gateway import PaymentGatewayEmulator


async def test_always_succeeds_when_rate_is_one() -> None:
    gateway = PaymentGatewayEmulator(min_delay=0, max_delay=0, success_rate=1.0)
    results = [await gateway.charge(uuid.uuid4()) for _ in range(20)]
    assert set(results) == {PaymentStatus.SUCCEEDED}


async def test_always_fails_when_rate_is_zero() -> None:
    gateway = PaymentGatewayEmulator(min_delay=0, max_delay=0, success_rate=0.0)
    results = [await gateway.charge(uuid.uuid4()) for _ in range(20)]
    assert set(results) == {PaymentStatus.FAILED}


async def test_returns_only_terminal_statuses() -> None:
    gateway = PaymentGatewayEmulator(min_delay=0, max_delay=0, success_rate=0.5)
    results = {await gateway.charge(uuid.uuid4()) for _ in range(50)}
    assert results <= {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}
    assert PaymentStatus.PENDING not in results


@pytest.mark.parametrize("bounds", [(0.0, 0.0), (0.01, 0.02)])
async def test_delay_stays_within_bounds(bounds: tuple[float, float]) -> None:
    import time

    low, high = bounds
    gateway = PaymentGatewayEmulator(min_delay=low, max_delay=high, success_rate=1.0)
    started = time.monotonic()
    await gateway.charge(uuid.uuid4())
    elapsed = time.monotonic() - started
    assert elapsed >= low
    assert elapsed < high + 1.0
