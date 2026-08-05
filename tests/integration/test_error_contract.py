"""Единый формат ошибок: даже неожиданный сбой отвечает JSON, а не text/plain."""

from typing import Any

import httpx
import pytest
import pytest_asyncio

from app.main import create_app
from app.schemas.payments import MAX_WEBHOOK_URL_LENGTH

PATH = "/api/v1/payments"
API_KEY = "test-api-key"


class BrokenService:
    """Сервис, падающий так, как это сделала бы недоступная БД."""

    async def create_payment(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("connection to server was lost; params=('secret-key', '1499.90')")

    async def get_payment(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("connection to server was lost")


@pytest_asyncio.fixture
async def broken_client() -> Any:
    app = create_app()
    app.state.payment_service = BrokenService()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client


async def test_unhandled_error_returns_json(
    broken_client: Any, payment_payload: dict[str, Any]
) -> None:
    response = await broken_client.post(
        PATH, json=payment_payload, headers={"Idempotency-Key": "boom"}
    )

    assert response.status_code == 500
    assert response.headers["content-type"].startswith("application/json")
    assert response.json() == {"detail": "Internal server error"}


async def test_unhandled_error_does_not_leak_internals(
    broken_client: Any, payment_payload: dict[str, Any]
) -> None:
    """Текст исключения может содержать параметры SQL, наружу он не идёт."""
    response = await broken_client.post(
        PATH, json=payment_payload, headers={"Idempotency-Key": "boom"}
    )

    body = response.text
    for secret in ("connection to server", "secret-key", "1499.90", "Traceback"):
        assert secret not in body


async def test_error_on_get_is_also_json(broken_client: Any) -> None:
    import uuid

    response = await broken_client.get(f"{PATH}/{uuid.uuid4()}")

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal server error"}


async def test_unhandled_error_is_logged_structurally(
    broken_client: Any, payment_payload: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level("ERROR", logger="app.main"):
        await broken_client.post(PATH, json=payment_payload, headers={"Idempotency-Key": "boom"})

    record = next(r for r in caplog.records if r.message == "unhandled error")
    assert record.ctx["method"] == "POST"
    assert record.ctx["path"] == PATH
    assert record.exc_info is not None


@pytest.mark.parametrize("length", [MAX_WEBHOOK_URL_LENGTH + 1, 2083])
async def test_overlong_webhook_url_is_422_not_500(
    api_client: Any, payment_payload: dict[str, Any], length: int
) -> None:
    """Раньше такой URL проходил валидацию и падал на INSERT голым 500."""
    prefix = "https://receiver.test/"
    url = prefix + "x" * (length - len(prefix))
    assert len(url) == length

    response = await api_client.post(
        PATH, json={**payment_payload, "webhook_url": url}, headers={"Idempotency-Key": "long"}
    )

    assert response.status_code == 422
    assert "detail" in response.json()


async def test_url_at_storage_limit_is_accepted(
    api_client: Any, payment_payload: dict[str, Any]
) -> None:
    prefix = "https://receiver.test/"
    url = prefix + "x" * (MAX_WEBHOOK_URL_LENGTH - len(prefix))

    response = await api_client.post(
        PATH, json={**payment_payload, "webhook_url": url}, headers={"Idempotency-Key": "limit"}
    )

    assert response.status_code == 202
