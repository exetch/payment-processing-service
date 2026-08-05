import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Numeric, String, Text, func, text
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Currency(enum.StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(enum.StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _enum_values(enum_cls: type[enum.StrEnum]) -> list[str]:
    """В БД храним значения enum'ов (lowercase), а не имена членов."""
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"
    # INSERT ... RETURNING сразу отдаёт server_default'ы (id, status, created_at)
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        SAEnum(Currency, name="currency", values_callable=_enum_values), nullable=False
    )
    description: Mapped[str | None] = mapped_column(Text())
    # Имя `metadata` занято Declarative API: атрибут metadata_, колонка "metadata"
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(PaymentStatus, name="payment_status", values_callable=_enum_values),
        nullable=False,
        server_default=text("'pending'"),
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Валидации Pydantic мало: она не защищает от записи в обход API
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)

    def __repr__(self) -> str:
        return f"<Payment {self.id} {self.status}>"
