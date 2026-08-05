import httpx
import pytest

from app.workers.webhooks import WebhookDeliveryError, WebhookSender

URL = "http://receiver.test/webhook"
PAYLOAD = {"event": "payment.processed", "status": "succeeded"}


def _sender(handler: object) -> WebhookSender:
    transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
    return WebhookSender(
        httpx.AsyncClient(transport=transport),
        timeout=1.0,
        signing_secret="unit-secret",
        allowed_hosts=["receiver.test"],
    )


@pytest.mark.parametrize("status_code", [200, 201, 202, 204, 299])
async def test_any_2xx_counts_as_delivered(status_code: int) -> None:
    sender = _sender(lambda request: httpx.Response(status_code))
    await sender.send(URL, PAYLOAD)


@pytest.mark.parametrize("status_code", [301, 302, 400, 404, 429, 500, 503])
async def test_non_2xx_raises(status_code: int) -> None:
    sender = _sender(lambda request: httpx.Response(status_code))
    with pytest.raises(WebhookDeliveryError, match=str(status_code)):
        await sender.send(URL, PAYLOAD)


async def test_network_error_raises_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns failure", request=request)

    sender = _sender(handler)
    with pytest.raises(WebhookDeliveryError, match="network error"):
        await sender.send(URL, PAYLOAD)


async def test_timeout_raises_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    sender = _sender(handler)
    with pytest.raises(WebhookDeliveryError, match="network error"):
        await sender.send(URL, PAYLOAD)


async def test_payload_is_sent_as_json_body() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.content
        seen["content_type"] = request.headers.get("content-type")
        return httpx.Response(200)

    await _sender(handler).send(URL, PAYLOAD)
    assert seen["url"] == URL
    assert seen["content_type"] == "application/json"
    assert b'"payment.processed"' in seen["body"]  # type: ignore[operator]


async def test_original_error_is_chained() -> None:
    """Причина сбоя должна доезжать до логов, а не подменяться."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns failure", request=request)

    with pytest.raises(WebhookDeliveryError) as info:
        await _sender(handler).send(URL, PAYLOAD)
    assert isinstance(info.value.__cause__, httpx.ConnectError)
