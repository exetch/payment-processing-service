"""Общие настройки тестов.

Заглушки окружения выставляются до импорта app-модулей: настройки читаются
на уровне модуля, иначе импорт упал бы на отсутствующих переменных.
Инфраструктурные фикстуры (контейнеры, БД, брокер) — в tests/integration/conftest.py.
"""

import os
from typing import Any

import pytest

# Ryuk (контейнер-уборщик testcontainers) не запускается на части хостов из-за
# проброса порта 8080; контейнеры и без него останавливаются своими with-блоками.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:1/test")
os.environ.setdefault("RABBITMQ_URL", "amqp://guest:guest@localhost:1/")
os.environ.setdefault("WEBHOOK_SIGNING_SECRET", "test-signing-secret")

API_KEY = "test-api-key"
SIGNING_SECRET = "test-signing-secret"


@pytest.fixture
def payment_payload() -> dict[str, Any]:
    return {
        "amount": "1499.90",
        "currency": "RUB",
        "description": "Заказ #42",
        "metadata": {"order_id": 42, "customer": "ivan"},
        "webhook_url": "http://receiver.test/webhook",
    }
