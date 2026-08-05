"""Проверки адреса назначения webhook'ов."""

import pytest

from app.core.net import UnsafeWebhookTargetError, check_resolved_host, check_url_literal


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "http://127.1.2.3/hook",
        "https://10.0.0.5/hook",
        "http://192.168.1.1:8080/hook",
        "http://172.16.0.1/hook",
        "http://169.254.169.254/latest/meta-data/",  # метаданные облака
        "http://0.0.0.0/hook",
        "http://[::1]/hook",
        "http://[fe80::1]/hook",
        "http://224.0.0.1/hook",
    ],
)
def test_blocked_ip_literals_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeWebhookTargetError):
        check_url_literal(url)


@pytest.mark.parametrize(
    "url",
    ["https://8.8.8.8/hook", "http://93.184.216.34/hook", "https://[2606:4700::1111]/hook"],
)
def test_public_ip_literals_pass(url: str) -> None:
    check_url_literal(url)


@pytest.mark.parametrize(
    "url", ["https://example.com/hook", "http://receiver.test/webhook", "https://sub.domain.io/x"]
)
def test_domain_names_are_deferred_to_delivery(url: str) -> None:
    """Литеральная проверка не резолвит DNS — это работа проверки перед отправкой."""
    check_url_literal(url)


def test_url_without_host_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookTargetError, match="no host"):
        check_url_literal("http:///hook")


async def test_resolved_loopback_is_rejected() -> None:
    """localhost — доменное имя, поэтому ловится только резолвингом."""
    with pytest.raises(UnsafeWebhookTargetError, match="blocked address"):
        await check_resolved_host("http://localhost:9000/hook")


async def test_unresolvable_host_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookTargetError, match="not resolvable"):
        await check_resolved_host("http://no-such-host.invalid/hook")


async def test_public_host_passes_resolution() -> None:
    await check_resolved_host("http://8.8.8.8/hook")


def test_allowlist_exempts_only_named_host() -> None:
    """Исключение точечное: остальные приватные адреса остаются запрещёнными."""
    allowed = ["webhook-sink"]
    check_url_literal("http://webhook-sink:9000/webhook", allowed)

    with pytest.raises(UnsafeWebhookTargetError):
        check_url_literal("http://169.254.169.254/latest/meta-data/", allowed)
    with pytest.raises(UnsafeWebhookTargetError):
        check_url_literal("http://127.0.0.1/webhook", allowed)


async def test_allowlist_skips_resolution_for_named_host() -> None:
    """Разрешённый хост не обязан резолвиться снаружи — он внутренний."""
    await check_resolved_host("http://webhook-sink:9000/webhook", ["webhook-sink"])

    with pytest.raises(UnsafeWebhookTargetError):
        await check_resolved_host("http://localhost:9000/hook", ["webhook-sink"])
