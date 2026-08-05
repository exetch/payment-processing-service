"""Эмулятор внешнего платёжного шлюза."""

import asyncio
import logging
import random
import uuid

from app.models import PaymentStatus

logger = logging.getLogger(__name__)


class PaymentGatewayEmulator:
    """Эмуляция обработки: задержка 2–5 с, ~90% успеха (параметры из настроек).

    ``failed`` это бизнес-результат платежа, а не сбой обработки: ретраев не вызывает.
    """

    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate

    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        delay = random.uniform(self._min_delay, self._max_delay)
        await asyncio.sleep(delay)
        status = (
            PaymentStatus.SUCCEEDED
            if random.random() < self._success_rate
            else PaymentStatus.FAILED
        )
        logger.info(
            "gateway emulation finished",
            extra={
                "ctx": {
                    "payment_id": str(payment_id),
                    "result": status.value,
                    "delay_seconds": round(delay, 2),
                }
            },
        )
        return status
