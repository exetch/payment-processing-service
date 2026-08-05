"""Проверки адреса назначения webhook'ов (защита от SSRF).

Клиент задаёт ``webhook_url`` произвольно, а запрос уходит из внутренней сети,
поэтому доставка на loopback, приватные и служебные диапазоны запрещена: иначе
через сервис можно достучаться до соседних контейнеров, метаданных облака
(169.254.169.254) и прочей инфраструктуры.
"""

import asyncio
import ipaddress
import socket
from collections.abc import Collection
from urllib.parse import urlparse


class UnsafeWebhookTargetError(ValueError):
    """Адрес назначения запрещён политикой, ретраи бессмысленны."""


def _is_blocked(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def check_url_literal(url: str, allowed_hosts: Collection[str] = ()) -> None:
    """Синхронная проверка без DNS: отсекает адрес, записанный прямо в URL.

    Нужна на входе API, чтобы клиент сразу получил 422 вместо принятого платежа,
    который потом не доставится.
    """
    host = urlparse(url).hostname
    if host is None:
        raise UnsafeWebhookTargetError("webhook_url has no host")
    if host in allowed_hosts:
        return
    try:
        blocked = _is_blocked(host)
    except ValueError:
        return  # не IP-литерал, а доменное имя, проверим при доставке
    if blocked:
        raise UnsafeWebhookTargetError(f"host {host} is not allowed")


async def check_resolved_host(url: str, allowed_hosts: Collection[str] = ()) -> None:
    """Проверка перед самой отправкой: резолвим имя и смотрим на реальные адреса.

    Отдельно от ``check_url_literal``, потому что домен может указывать на
    приватный адрес и менять его между созданием платежа и доставкой.
    """
    host = urlparse(url).hostname
    if host is None:
        raise UnsafeWebhookTargetError("webhook_url has no host")
    if host in allowed_hosts:
        return
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise UnsafeWebhookTargetError(f"host {host} is not resolvable: {exc}") from exc
    for info in infos:
        address = str(info[4][0])
        if _is_blocked(address):
            raise UnsafeWebhookTargetError(f"host {host} resolves to blocked address {address}")
