import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.models import Currency, Payment, PaymentStatus

MAX_WEBHOOK_URL_LENGTH = 2048


class PaymentCreate(BaseModel):
    """Тело POST /api/v1/payments."""

    model_config = ConfigDict(str_strip_whitespace=True)

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["199.99"])
    currency: Currency
    description: str | None = Field(default=None, max_length=1024)
    metadata: dict[str, Any] | None = Field(
        default=None, description="Произвольные метаданные платежа (JSON-объект)"
    )
    webhook_url: HttpUrl = Field(description="URL для уведомления о результате обработки")

    @field_validator("webhook_url")
    @classmethod
    def _fits_storage(cls, value: HttpUrl) -> HttpUrl:
        if len(str(value)) > MAX_WEBHOOK_URL_LENGTH:
            raise ValueError(f"webhook_url must be at most {MAX_WEBHOOK_URL_LENGTH} characters")
        return value


class PaymentCreated(BaseModel):
    """Ответ 202 на создание платежа."""

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentOut(BaseModel):
    """Детальная карточка платежа."""

    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentOut":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


class ErrorResponse(BaseModel):
    """Единый формат ошибок API."""

    detail: str
