"""Отправка webhook-уведомлений клиенту."""

import hashlib
import hmac
import json
import logging
import time
from collections.abc import Collection
from typing import Any

import httpx

from app.core.net import check_resolved_host

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Webhook-Signature"


class WebhookDeliveryError(RuntimeError):
    """Webhook не доставлен: сетевая ошибка или не-2xx ответ получателя."""


def build_signature(secret: str, timestamp: int, body: bytes) -> str:
    """Подпись по схеме Stripe: HMAC-SHA256 от ``<timestamp>.<тело>``.

    Timestamp входит в подписываемую строку, поэтому перехваченное уведомление
    нельзя переигрывать бесконечно: получатель отвергает старые метки.
    """
    signed = f"{timestamp}.".encode() + body
    return hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


class WebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        timeout: float,
        signing_secret: str,
        allowed_hosts: Collection[str] = (),
    ) -> None:
        self._client = client
        self._timeout = timeout
        self._signing_secret = signing_secret
        self._allowed_hosts = allowed_hosts

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        # Поднимет UnsafeWebhookTargetError: постоянный отказ, без ретраев
        await check_resolved_host(url, self._allowed_hosts)

        # Подписываем ровно те байты, которые уйдут в сеть
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        timestamp = int(time.time())
        signature = build_signature(self._signing_secret, timestamp, body)
        headers = {
            "Content-Type": "application/json",
            SIGNATURE_HEADER: f"t={timestamp},v1={signature}",
        }
        try:
            response = await self._client.post(
                url, content=body, headers=headers, timeout=self._timeout
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"network error: {exc}") from exc
        if response.status_code // 100 != 2:
            raise WebhookDeliveryError(f"unexpected status {response.status_code}")
        logger.info(
            "webhook delivered",
            extra={"ctx": {"url": url, "status_code": response.status_code}},
        )
