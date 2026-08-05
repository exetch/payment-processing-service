"""Проверка статического API-ключа."""

import secrets


def api_key_is_valid(provided: str, expected: str) -> bool:
    """Сравнение за константное время."""
    return secrets.compare_digest(provided.encode(), expected.encode())
