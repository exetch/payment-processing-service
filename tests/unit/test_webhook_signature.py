"""Подпись webhook-уведомлений."""

import hashlib
import hmac
import json
import time

import httpx
import pytest

from app.core.net import UnsafeWebhookTargetError
from app.workers.webhooks import SIGNATURE_HEADER, WebhookSender, build_signature

SECRET = "signing-secret"
URL = "https://receiver.test/webhook"
PAYLOAD = {"event_id": "evt-1", "status": "succeeded", "amount": "10.00"}


def _capturing_sender(
    seen: dict, allowed_hosts: tuple[str, ...] = ("receiver.test",)
) -> WebhookSender:
    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = request.content
        seen["signature"] = request.headers.get(SIGNATURE_HEADER)
        seen["content_type"] = request.headers.get("content-type")
        return httpx.Response(200)

    return WebhookSender(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        timeout=1.0,
        signing_secret=SECRET,
        allowed_hosts=allowed_hosts,
    )


def test_signature_matches_reference_implementation() -> None:
    body = b'{"a":1}'
    expected = hmac.new(SECRET.encode(), b"1700000000." + body, hashlib.sha256).hexdigest()
    assert build_signature(SECRET, 1700000000, body) == expected


def test_signature_depends_on_body_and_timestamp() -> None:
    base = build_signature(SECRET, 100, b"body")
    assert build_signature(SECRET, 100, b"body!") != base
    assert build_signature(SECRET, 101, b"body") != base
    assert build_signature("other-secret", 100, b"body") != base


async def test_request_carries_signature_header() -> None:
    seen: dict = {}
    await _capturing_sender(seen).send(URL, PAYLOAD)

    assert seen["content_type"] == "application/json"
    parts = dict(part.split("=", 1) for part in seen["signature"].split(","))
    assert set(parts) == {"t", "v1"}
    assert abs(time.time() - int(parts["t"])) < 30


async def test_signature_verifies_against_exact_bytes_sent() -> None:
    """Подписываются именно отправленные байты, а не пересобранный заново JSON."""
    seen: dict = {}
    await _capturing_sender(seen).send(URL, PAYLOAD)

    parts = dict(part.split("=", 1) for part in seen["signature"].split(","))
    expected = build_signature(SECRET, int(parts["t"]), seen["body"])
    assert hmac.compare_digest(expected, parts["v1"])
    assert json.loads(seen["body"]) == PAYLOAD


async def test_unicode_payload_is_signed_correctly() -> None:
    seen: dict = {}
    await _capturing_sender(seen).send(URL, {"description": "Заказ №42 — оплата"})

    parts = dict(part.split("=", 1) for part in seen["signature"].split(","))
    assert build_signature(SECRET, int(parts["t"]), seen["body"]) == parts["v1"]
    assert json.loads(seen["body"])["description"] == "Заказ №42 — оплата"


async def test_private_target_is_refused_before_request() -> None:
    seen: dict = {}
    sender = _capturing_sender(seen, allowed_hosts=())

    with pytest.raises(UnsafeWebhookTargetError):
        await sender.send("http://localhost:9000/hook", PAYLOAD)

    assert seen == {}, "запрос не должен уходить вовсе"
