"""Конфигурация приложения; единственное место чтения переменных окружения."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_key: str
    database_url: str
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    log_level: str = "INFO"

    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = 0.9

    webhook_timeout_seconds: float = 5.0
    # Секрет подписи уведомлений; обязателен, потому что молча слать неподписанные webhook'и хуже,
    # чем не стартовать
    webhook_signing_secret: str
    # Точечные исключения из защиты от SSRF: хосты через запятую, которым разрешено
    # быть во внутренней сети. Не флаг «выключить всё»: остальные приватные адреса
    # (включая 169.254.169.254) остаются запрещены даже в демо
    webhook_allowed_hosts: str = ""

    consumer_max_retries: int = 3
    retry_base_delay_seconds: float = 2.0

    outbox_poll_interval_seconds: float = 0.5
    outbox_batch_size: int = 100
    # Backoff для строки, которую не удалось опубликовать: она откладывается,
    # а relay продолжает с остальными
    outbox_retry_base_delay_seconds: float = 1.0
    outbox_retry_max_delay_seconds: float = 60.0

    @property
    def webhook_allowed_host_set(self) -> frozenset[str]:
        return frozenset(
            host.strip() for host in self.webhook_allowed_hosts.split(",") if host.strip()
        )

    @property
    def retry_delays_seconds(self) -> list[float]:
        """Экспоненциальная лестница задержек: base * 2**n → 2s, 4s, 8s при дефолтах."""
        return [self.retry_base_delay_seconds * 2**n for n in range(self.consumer_max_retries)]


@lru_cache
def get_settings() -> Settings:
    return Settings()
