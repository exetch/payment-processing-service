import pytest

from app.core.config import Settings


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "api_key": "k",
        "database_url": "postgresql+asyncpg://u:p@h/db",
        "webhook_signing_secret": "s",
    }
    return Settings(**{**base, **overrides})  # type: ignore[arg-type]


def test_retry_ladder_is_exponential() -> None:
    settings = _settings(retry_base_delay_seconds=2, consumer_max_retries=3)
    assert settings.retry_delays_seconds == [2, 4, 8]


@pytest.mark.parametrize(
    ("base", "retries", "expected"),
    [
        (1, 1, [1]),
        (0.5, 3, [0.5, 1.0, 2.0]),
        (2, 5, [2, 4, 8, 16, 32]),
        (2, 0, []),
    ],
)
def test_retry_ladder_follows_settings(
    base: float, retries: int, expected: list[float]
) -> None:
    settings = _settings(retry_base_delay_seconds=base, consumer_max_retries=retries)
    assert settings.retry_delays_seconds == expected


def test_ladder_length_matches_max_retries() -> None:
    """Тир для каждой попытки обязан существовать: processor индексирует список."""
    settings = _settings(consumer_max_retries=4)
    for attempt in range(1, settings.consumer_max_retries + 1):
        assert settings.retry_delays_seconds[attempt - 1] > 0


def test_api_key_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)
    with pytest.raises(ValueError, match="api_key"):
        Settings(  # type: ignore[call-arg]
            database_url="postgresql+asyncpg://u:p@h/db", webhook_signing_secret="s"
        )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", set()),
        ("webhook-sink", {"webhook-sink"}),
        ("a, b ,c", {"a", "b", "c"}),
        (" , ", set()),
    ],
)
def test_allowed_hosts_are_parsed(raw: str, expected: set[str]) -> None:
    assert _settings(webhook_allowed_hosts=raw).webhook_allowed_host_set == expected
